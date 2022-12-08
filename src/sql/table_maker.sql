create type StaffType as enum('SustcManager', 'CompanyManager', 'Courier', 'SeaportOfficer');
create type ContainerType as enum('Dry', 'FlatRack', 'ISOTank', 'OpenTop', 'Reefer');
create type ItemStateType as enum('PickingUp','ToExportTransporting', 'ExportChecking', 'ExportCheckFailed', 'PackingToContainer', 'WaitingForShipping', 'Shipping', 'UnpackingFromContainer', 'ImportChecking', 'ImportCheckFailed', 'FromImportTransporting', 'Delivering', 'Finish');
create table if not exists staff (
    name varchar not null,
    password varchar not null,
    type StaffType not null,
    city varchar,
    gender boolean not null,
    phone_number varchar not null,
    birth_year integer not null,
    company varchar,
    primary key (name)
);

create table if not exists export_information (
    item_name varchar not null,
    city varchar not null,
    tax numeric(20, 7) not null,
    staff_name varchar references staff(name),
    primary key (item_name)
);

create table if not exists import_information(
    item_name varchar not null,
    city varchar not null,
    tax numeric(20, 7) not null,
    staff_name varchar references staff(name),
    primary key (item_name)
);

create table if not exists ship(
    item_name varchar not null,
    ship_name varchar,
    company varchar not null,
    primary key (item_name)
);


create table if not exists container(
    item_name varchar not null,
    code varchar,
    type ContainerType,
    primary key (item_name)
);

create table if not exists retrieval_information(
    item_name varchar not null,
    city varchar not null,
    staff_name varchar not null references staff(name),
    primary key (item_name)
);

create table if not exists delivery_information(
    item_name varchar not null,
    city varchar not null,
    staff_name varchar references staff(name),
    primary key (item_name)
);

create table if not exists item(
    name varchar not null,
    type varchar not null,
    price numeric(20, 7) not null,
    state ItemStateType not null,

    primary key (name)
);

alter table delivery_information add constraint  ForeignKey_DeliveryInformation_ItemName foreign key (item_name) references item(name);
alter table retrieval_information add constraint  ForeignKey_RetrievalInformation_ItemName foreign key (item_name) references item(name);
alter table export_information add constraint  ForeignKey_ExportInformation_ItemName foreign key (item_name) references item(name);
alter table import_information add constraint  ForeignKey_ImportInformation_ItemName foreign key (item_name) references item(name);
alter table ship add constraint  ForeignKey_Ship_ItemName foreign key (item_name) references item(name);
alter table container add constraint ForeignKey_Container_ItemName foreign key (item_name) references item(name);