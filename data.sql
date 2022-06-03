/*
Copyright SEETHRU GmbH.
Licensed under the EUPL-1.2 or later.
*/

drop view if exists v_history cascade;
drop view if exists v_balance;
drop view if exists v_balance_data cascade;
drop view if exists v_kpi cascade;
drop index if exists history_document_idx;
drop index if exists history_account_idx;
drop index if exists history_party_idx;
drop table if exists history;
drop table if exists document;
drop table if exists party;
drop table if exists account;
drop table if exists journal;

drop index if exists kpidata_kpi_idx;
drop table if exists kpidata;
drop table if exists kpi;

create table account (
id varchar,
header boolean default false,
level smallint,
name varchar,
category varchar,
primary key(id)
);

create table party (
id varchar,
name varchar,
customer boolean default false,
supplier boolean default false,
category varchar,
primary key(id)
);

create table journal (
id varchar,
name varchar,
category varchar,
primary key(id)
);

create table document (
id int,
period_id int,
journal_id varchar,
number int,
dt date,
duedt date,
description varchar,
primary key(id)
);

create table history (
id int,
document_id int,
account_id varchar,
party_id varchar,
comment varchar,
debit numeric(15,2),
credit numeric(15,2),
tallied boolean default false,
tally_number int,
constraint fk_document
    foreign key(document_id)
    references document(id),
constraint fk_account
    foreign key(account_id)
    references account(id),
constraint fk_party
    foreign key(party_id)
    references party(id),
primary key(id)
);
create index history_document_idx on history(document_id);
create index history_account_idx on history(account_id);
create index history_party_idx on history(party_id);

create view v_history as
select d.period_id/100 as period_year, mod(period_id,100) as period_month, (period_id/100)*10 + mod(period_id,100)/4 + 1 as period_quarter, d.period_id, 
d.journal_id, d.number, j.category as journal_category, j.name as journal_name, 
d.dt, d.description,
h.account_id, a.name as account_name, h.account_id || ' - ' || a.name as account_label, a.category as account_category,
left(account_id, 1) as account_class1, coalesce(cl1.name,'') as account_class1_name, coalesce(left(account_id, 1) || ' - ' || cl1.name,'') as account_class1_label,
left(account_id, 2) as account_class2, coalesce(cl2.name,'') as account_class2_name, coalesce(left(account_id, 2) || ' - ' || cl2.name,'') as account_class2_label, 
left(account_id, 3) as account_class3, coalesce(cl3.name,'') as account_class3_name, coalesce(left(account_id, 3) || ' - ' || cl3.name,'') as account_class3_label, 
h.party_id, p.name as party_name, h.party_id || ' - ' || p.name as party_label, p.category as party_category, p.customer as party_customer, p.supplier as party_supplier,
h.debit, h.credit, h.credit-h.debit as credit_debit, h.debit-h.credit as debit_credit, h.debit+h.credit as balance,
h.tallied, h.tally_number
from document as d 
inner join history as h on d.id=h.document_id
left join account as a on a.id=h.account_id
left join party as p on p.id=h.party_id
left join journal as j on d.journal_id=j.id
left join account as cl1 on cl1.id=left(h.account_id,1)
left join account as cl2 on cl2.id=left(h.account_id,2)
left join account as cl3 on cl3.id=left(h.account_id,3);

create view v_history_cost as
select *, debit_credit as amount_cost, 0 as amount_revenue from v_history where ({{ model.v_history_cost.account_filter }});

create view v_history_revenue as
select *, 0 as amount_cost, credit_debit as amount_revenue from v_history where ({{ model.v_history_revenue.account_filter }});

create view v_history_profit_loss as
select * from v_history_cost
union all
select * from v_history_revenue;

create view v_history_party as
select * from v_history where account_id is null and party_id is not null;

create view v_history_customer as
select * from v_history where account_id is null and party_id is not null and party_customer=true;

create view v_history_supplier as
select * from v_history where account_id is null and party_id is not null and party_supplier=true;

/*
create view v_balance_data as
select d.period_id, 
h.account_id, 
sum(h.debit) as debit, sum(h.credit) as credit, sum(h.credit-h.debit) as credit_debit, sum(h.debit-h.credit) as debit_credit, sum(h.debit+h.credit) as balance
from document as d 
inner join history as h on d.id=h.document_id
where h.account_id is not null
group by period_id, h.account_id;
*/

create view v_balance as
select period_id/100 as period_year, mod(period_id,100) as period_month, (period_id/100)*10 + mod(period_id,100)/4 + 1 as period_quarter, period_id, 
b.account_id, a.name as account_name, b.account_id || ' - ' || a.name as account_label, a.category as account_category,
left(account_id, 1) as account_class1, coalesce(cl1.name,'') as account_class1_name, coalesce(left(account_id, 1) || ' - ' || cl1.name,'') as account_class1_label,
left(account_id, 2) as account_class2, coalesce(cl2.name,'') as account_class2_name, coalesce(left(account_id, 2) || ' - ' || cl2.name,'') as account_class2_label, 
left(account_id, 3) as account_class3, coalesce(cl3.name,'') as account_class3_name, coalesce(left(account_id, 3) || ' - ' || cl3.name,'') as account_class3_label,
b.debit, b.credit, b.debit_credit , b.credit_debit, b.balance,
coalesce((select sum(debit) from history as x inner join document d on d.id=x.document_id  where x.account_id=b.account_id and d.period_id=b.period_id-100),0) as debit1,
coalesce((select sum(credit) from history as x inner join document d on d.id=x.document_id  where x.account_id=b.account_id and d.period_id=b.period_id-100),0) as credit1,
coalesce((select sum(debit-credit) from history as x inner join document d on d.id=x.document_id  where x.account_id=b.account_id and d.period_id=b.period_id-100),0) as debit_credit1,
coalesce((select sum(credit-debit) from history as x inner join document d on d.id=x.document_id  where x.account_id=b.account_id and d.period_id=b.period_id-100),0) as credit_debit1,
coalesce((select sum(debit+credit) from history as x inner join document d on d.id=x.document_id  where x.account_id=b.account_id and d.period_id=b.period_id-100),0) as balance1
from (
    select d.period_id, 
h.account_id, 
sum(h.debit) as debit, sum(h.credit) as credit, sum(h.credit-h.debit) as credit_debit, sum(h.debit-h.credit) as debit_credit, sum(h.debit+h.credit) as balance
from document as d 
inner join history as h on d.id=h.document_id
where h.account_id is not null
group by period_id, h.account_id
) as b
left join account as a on a.id=b.account_id
left join account as cl1 on cl1.id=left(b.account_id,1)
left join account as cl2 on cl2.id=left(b.account_id,2)
left join account as cl3 on cl3.id=left(b.account_id,3);

create table kpi (
id varchar,
name varchar,
unit varchar,
primary key(id)
);

insert into kpi(id, name, unit) values 
('financial.cost.total','Charges', 'amount'),
('financial.cost.sales','Charges marchandises', 'amount'),
('financial.cost.overhead','Charges frais généraux', 'amount'),
('financial.cost.staff','Charges personnel', 'amount'),
('financial.revenue.total','Revenus', 'amount'),
('financial.revenue.sales','Chiffre d''affaire', 'amount'),
('financial.revenue.other','Autres revenus', 'amount'),
('financial.profit.net','Bénéfice net', 'amount'),
('financial.profit.gross','Bénéfice brut', 'amount'),
('financial.profit.addedvalue','Valeur ajoutée', 'amount'),
('financial.margin.net','Marge net ', 'percentage'),
('financial.margin.gross','Marge brut', 'percentage'),
('financial.liquidity','Liquidité', 'ratio'),
('financial.solvency','Solvabilité', 'ratio');

create table kpidata (
id int,
kpi_id varchar,
period_id int,
value numeric(15,4),
constraint fk_kpi
    foreign key(kpi_id)
    references kpi(id),
primary key(id)
);

create index kpidata_kpi_idx on kpi(id);

create view v_kpi as
select m.period_id/100 as period_year, mod(m.period_id,100) as period_month, (period_id/100)*10 + mod(period_id,100)/4 + 1 as period_quarter, m.period_id, 
/* make_date(period_id/100, mod(period_id,100), 1) as dt, */
date_trunc('month', make_date(period_id/100, mod(period_id,100), 1)) + interval '1 month - 1 day' as dt,
k.id as kpi_id, k.name as kpi_name, k.unit as kpi_unit,
m.value, coalesce((select value from kpidata as x where x.kpi_id = k.id and m.period_id=x.period_id+100),0) as value1
from kpi as k inner join kpidata as m on k.id=m.kpi_id;