# Copyright SEETHRU GmbH.
# Licensed under the EUPL-1.2 or later

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
import yaml
import sys
import csv
from datetime import datetime
from decimal import Decimal
from jinja2 import Template
import logging

Journal = None
Party = None
Document = None
History = None
Account = None
Journal = None
Kpi = None
KpiData = None


def load_config(config_file):
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', filename='openfinesse.log', filemode='w', encoding='utf-8', level=logging.DEBUG)
    logging.debug('Parsing config file: {}'.format(config_file))
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
        return config


class BaseImporter():

    def __init__(self, config):
        self.config = config
        self.accounts = dict()
        self.parties = dict()
        self.journals = dict()
        self.documents = dict()
        self.kpis = dict()
        self.kpidata = dict()
        self.next_document_id = 0
        self.next_history_id = 0
        self.next_kpidata_id = 0


    def init_database(self):
        global Journal, Party, Document, History, Account, Journal, Kpi, KpiData
        print('Initialising database')

        connection_string = self.config['model']['connection']
        logging.debug('Connecting to database: {}'.format(connection_string))
        self.engine = sqlalchemy.create_engine(connection_string)
        self.session = Session(self.engine)
        self.base = automap_base()
        self.base.prepare(self.engine, reflect=True)

        filename = self.config['model'].get('template', 'data.sql')
        logging.debug('Executing database initialisation script: {}'.format(filename))
        with open(filename, 'r') as f:
            template = Template(f.read())
        sql = template.render(self.config)
        script = sqlalchemy.text(sql)
        self.engine.execute(script.execution_options(autocommit=True))
        
        Journal = self.base.classes.journal
        Party = self.base.classes.party
        Document = self.base.classes.document
        History = self.base.classes.history
        Account = self.base.classes.account
        Journal = self.base.classes.journal
        Kpi = self.base.classes.kpi
        KpiData = self.base.classes.kpidata


    def generate_kpi_data(self, kpi_id, query):
        parameters = self.config['kpi'].get(kpi_id)
        if parameters is not None and parameters.get('enable', True) is True:
            print('Calculating KPI', kpi_id)
            template = Template(query)
            sql = template.render(parameters)
            logging.debug('Calculating KPI {}: {}'.format(kpi_id, sql))
            rows = self.engine.execute(sql)
            for row in rows:
                self.next_kpidata_id += 1
                kpidata = KpiData(id=self.next_kpidata_id, kpi_id=kpi_id, period_id=row[0], value=row[1])
                self.session.add(kpidata)
            self.session.commit()
        else:
            print('Skipping KPI', kpi_id)


    def calculate_kpis(self):
        self.generate_kpi_data(
            'financial.cost.total', 
            "select period_id, sum(debit_credit) as amount from v_history_cost where ({{account_filter}}) group by period_id")
        
        self.generate_kpi_data(
            'financial.cost.sales', 
            "select period_id, sum(debit_credit) as amount from v_history_cost where ({{account_filter}}) group by period_id")
        
        self.generate_kpi_data(
            'financial.cost.overhead', 
            "select period_id, sum(debit_credit) as amount from v_history_cost where ({{account_filter}}) group by period_id")
        
        self.generate_kpi_data(
            'financial.cost.staff',
            "select period_id, sum(debit_credit) as amount from v_history_cost where ({{account_filter}}) group by period_id")
        
        self.generate_kpi_data(
            'financial.profit.gross', 
            "select period_id, sum(credit_debit) as amount from v_history where ({{account_filter}}) group by period_id")
        
        self.generate_kpi_data(
            'financial.profit.net', 
            "select period_id, sum(credit_debit) as amount from v_history_profit_loss group by period_id")
        
        self.generate_kpi_data(
            'financial.profit.addedvalue', 
            "select period_id, sum(credit_debit) from v_history where ({{account_filter}}) group by period_id")

        self.generate_kpi_data(
            'financial.revenue.total', 
            "select period_id, sum(credit_debit) as amount from v_history_revenue group by period_id")

        self.generate_kpi_data(
            'financial.revenue.sales', 
            "select period_id, sum(credit_debit) as amount from v_history_revenue where ({{account_filter}}) group by period_id")

        self.generate_kpi_data(
            'financial.revenue.other', 
            "select period_id, sum(credit_debit) as amount from v_history_revenue where ({{account_filter}}) group by period_id")
        
        self.generate_kpi_data(
            'financial.solvency', 
            "select period_id, " \
                "(select sum(credit_debit) from v_history where ({{account_filter_assets}}) and period_id = d.period_id) / " \
                "nullif((select sum(debit_credit) from v_history where ({{account_filter_liabilities}}) and period_id = d.period_id),0) " \
                "from document as d group by period_id")

        self.generate_kpi_data(
            # Quick Ratio = (Cash and Cash Equivalents + Marketable Securities + Accounts Receivable)/(Current Liabilities)
            'financial.liquidity', 
            "select period_id, 1 " \
                "from document as d group by period_id")

        # the following kpi value have dependencies on previous kpi calculations

        self.generate_kpi_data(
            'financial.margin.gross', 
            "select period_id, 100.0 * (select sum(value) from v_kpi " \
                "where kpi_id='financial.profit.gross' and period_id = d.period_id) / " \
                "nullif((select sum(value) from v_kpi where kpi_id='financial.revenue.sales' and period_id = d.period_id),0) " \
                "as value from kpidata as d group by period_id")

        self.generate_kpi_data(
            'financial.margin.net', 
            "select period_id, 100.0 * (select sum(value) from v_kpi " \
                "where kpi_id='financial.profit.net' and period_id = d.period_id) / " \
                "nullif((select sum(value) from v_kpi where kpi_id='financial.revenue.sales' and period_id = d.period_id),0) " \
                "as value from kpidata as d group by period_id")


class BobImporter(BaseImporter):

    def __init__(self, config):
        super().__init__(config)
        self.config_exclude_years = self.config['bob50'].get('exclude_years',[])
        logging.debug('Ignoring accounting years: {}'.format(self.config_exclude_years))


    def run(self):
        self.init_database()
        self.import_journals()
        self.import_accounts()
        self.import_parties()
        self.import_account_history()
        self.import_party_history()
        self.calculate_kpis()


    def import_parties(self):
        filename = self.config['bob50']['ac_compan']['file']
        with open(filename, 'r') as file:
            reader = csv.DictReader(file)
            count = 0
            for row in reader:
                count += 1
                key = row['CID']
                party = Party(id=key, 
                    name=row['CNAME1'],
                    customer = True if row['CCUSTYPE'] == 'C' else False,
                    supplier = True if row['CSUPTYPE'] == 'S' else False,
                    category = row['CCUSCAT'])
                self.session.add(party)
                self.parties[key] = party
            self.session.commit()
            print('Imported', count, 'parties')


    def import_accounts(self):
        filename = self.config['bob50']['ac_accoun']['file']
        with open(filename, 'r') as file:
            reader = csv.DictReader(file)
            count = 0
            for row in reader:
                count += 1
                key = row['AID']
                header = True if row['AISTITLE'] in ('true', 'True', '1') else False
                account = Account(id=key, header=header, name=row['LONGHEADING1'], category=row['ABALANCE'])
                self.session.add(account)
                self.accounts[key] = account
            self.session.commit()
            print('Imported', count, 'accounts')


    def import_journals(self):
        filename = self.config['bob50']['ac_dbk']['file']
        with open(filename, 'r') as file:
            reader = csv.DictReader(file)
            count = 0
            for row in reader:
                count += 1
                key = row['DBID']
                journal = Journal(id=key, name=row['HEADING1'], category=row['DBTYPE'])
                self.session.add(journal)
                self.journals[key] = journal
            self.session.commit()
            print('Imported', count, 'journals')


    def import_account_history(self):
        filename = self.config['bob50']['ac_ahisto']['file']
        with open(filename, 'r') as file:
            reader = csv.DictReader(file)
            count = 0
            for row in reader:
                count += 1
                if int(row['HYEAR']) in self.config_exclude_years:
                    continue
                key = (row['HYEAR'], row['HDBK'], row['HDOCNO']) 
                document = self.documents.get(key)
                if document is None:
                    self.next_document_id += 1
                    year = int(row['HYEAR'])
                    month = int(row['HMONTH'])
                    if month < 1:
                        month = 1
                    if month > 12:
                        month = 12
                    period_id = year*100 + month
                    dt = datetime.strptime(row['HDOCDATE'], '%Y-%m-%d')
                    document = Document(id=self.next_document_id, period_id=period_id, journal_id=row['HDBK'], number=row['HDOCNO'], dt=dt, description=row['HREM'])
                    self.session.add(document)
                self.next_history_id += 1
                account_id = row['HID']
                party_id = row['HCUSSUP'] if row['HCUSSUP'] != '' else None
                amount = Decimal(row['HAMOUNT'])
                debit = Decimal(0)
                credit = Decimal(0)
                if amount < 0:
                    credit = abs(amount)
                if amount > 0:
                    debit = amount
                tallied = True if row['HSTATUS'] == 'T' else False
                tally_number = int(row['HMATCHNO']) if row['HMATCHNO'] != '' else None
                history = History(id=self.next_history_id, document_id=document.id, account_id=account_id, party_id=party_id, debit=debit, credit=credit, tallied=tallied, tally_number=tally_number)
                self.session.add(history)
                self.documents[key] = document
            self.session.commit()
            print('Imported', count, 'account history records')


    def import_party_history(self):
        filename = self.config['bob50']['ac_chisto']['file']
        with open(filename, 'r') as file:
            reader = csv.DictReader(file)
            count = 0
            for row in reader:
                count += 1
                if int(row['HYEAR']) in self.config_exclude_years:
                    continue
                key = (row['HYEAR'], row['HDBK'], row['HDOCNO']) 
                document = self.documents.get(key)
                if document is None:
                    self.next_document_id += 1
                    year = int(row['HYEAR'])
                    month = int(row['HMONTH'])
                    if month < 1:
                        month = 1
                    if month > 12:
                        month = 12
                    period_id = year*100 + month 
                    dt = datetime.strptime(row['HDOCDATE'], '%Y-%m-%d')
                    document = Document(id=self.next_document_id, period_id=period_id, journal_id=row['HDBK'], number=row['HDOCNO'], dt=dt, description=row['HREMINT'])
                    self.session.add(document)
                self.next_history_id += 1
                party_id = row['HID']
                amount = Decimal(row['HAMOUNT'])
                debit = Decimal(0)
                credit = Decimal(0)
                if amount < 0:
                    credit = abs(amount)
                if amount > 0:
                    debit = amount
                tallied = True if row['HSTATUS'] == 'T' else False
                tally_number = int(row['HMATCHNO']) if row['HMATCHNO'] != '' else None
                history = History(id=self.next_history_id, document_id=document.id, party_id=party_id, debit=debit, credit=credit, tallied=tallied, tally_number=tally_number)
                self.session.add(history)
            self.session.commit()
            print('Imported', count, 'party history records')


def main():
    try:
        config_file = sys.argv[1]
        config = load_config(config_file)

        importer = None
        if config['source'] == 'bob50':
            importer = BobImporter(config)
        
        if importer is not None:
            importer.run()

    except Exception as e:
        print('An error has occured during the import')
        print('See openfinesse.log for more information')
        logging.error(e)

if __name__ == '__main__':
    main()