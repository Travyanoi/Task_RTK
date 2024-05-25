def create_models(cursor) -> None:
    cursor.execute("""create table if not exists okved_62_org_info(
                   id serial primary key not null,
                   company_name varchar(250) not null,
                   okved varchar(8) not null,
                   inn varchar(10) not null,
                   kpp varchar(9) not null,
                   registration_place varchar(200) not null);
                   """)
