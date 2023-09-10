CREATE SCHEMA IF NOT EXISTS cdm;

DROP TABLE IF EXISTS cdm.user_product_counters;
CREATE TABLE cdm.user_product_counters
(
    id           uuid         NOT NULL PRIMARY KEY,
    user_id      uuid         NOT NULL,
    product_id   uuid         NOT NULL,
    product_name varchar(100) NOT NULL,
    order_cnt    int          NOT NULL DEFAULT 0,

    CHECK ( order_cnt >= 0 )
);


DROP TABLE IF EXISTS cdm.user_category_counters;
CREATE TABLE cdm.user_category_counters
(
    id            uuid         NOT NULL PRIMARY KEY,
    user_id       uuid         NOT NULL,
    category_id   uuid         NOT NULL,
    category_name varchar(100) NOT NULL,
    order_cnt     integer      NOT NULL DEFAULT 0,
    CHECK (order_cnt >= 0)
);

CREATE SCHEMA IF NOT EXISTS stg;

DROP TABLE IF EXISTS stg.order_events;
CREATE TABLE stg.order_events
(
    id          int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_id   int                         NOT NULL UNIQUE,
    payload     json                        NOT NULL,
    object_type varchar(30)                 NOT NULL,
    sent_dttm   timestamp WITHOUT TIME ZONE NOT NULL
);


CREATE SCHEMA IF NOT EXISTS dds;

DROP TABLE IF EXISTS dds.h_user;
CREATE TABLE dds.h_user
(
    h_user_pk uuid                        NOT NULL PRIMARY KEY,
    user_id   varchar                     NOT NULL UNIQUE,
    load_dt   timestamp WITHOUT TIME ZONE NOT NULL,
    load_src  varchar                     NOT NULL
);


DROP TABLE IF EXISTS dds.h_product;
CREATE TABLE dds.h_product
(
    h_product_pk uuid                        NOT NULL PRIMARY KEY,
    product_id   varchar                     NOT NULL UNIQUE,
    load_dt      timestamp WITHOUT TIME ZONE NOT NULL,
    load_src     varchar                     NOT NULL
);


DROP TABLE IF EXISTS dds.h_category;
CREATE TABLE dds.h_category
(
    h_category_pk uuid                        NOT NULL PRIMARY KEY,
    category_name varchar                     NOT NULL UNIQUE,
    load_dt       timestamp WITHOUT TIME ZONE NOT NULL,
    load_src      varchar                     NOT NULL
);


DROP TABLE IF EXISTS dds.h_restaurant;
CREATE TABLE dds.h_restaurant
(
    h_restaurant_pk uuid                        NOT NULL PRIMARY KEY,
    restaurant_id   varchar                     NOT NULL UNIQUE,
    load_dt         timestamp WITHOUT TIME ZONE NOT NULL,
    load_src        varchar                     NOT NULL
);

DROP TABLE IF EXISTS dds.h_order;
CREATE TABLE dds.h_order
(
    h_order_pk uuid                        NOT NULL PRIMARY KEY,
    order_id   int                         NOT NULL UNIQUE,
    order_dt   timestamp WITHOUT TIME ZONE NOT NULL,
    load_dt    timestamp WITHOUT TIME ZONE NOT NULL,
    load_src   varchar                     NOT NULL
);



DROP TABLE IF EXISTS dds.l_order_product;
CREATE TABLE dds.l_order_product
(
    hk_order_product_pk uuid                        NOT NULL PRIMARY KEY,
    h_order_pk          uuid                        NOT NULL,
    h_product_pk        uuid                        NOT NULL,
    load_dt             timestamp WITHOUT TIME ZONE NOT NULL,
    load_src            varchar                     NOT NULL,

    FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk),
    FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk)
);


DROP TABLE IF EXISTS dds.l_product_restaurant;
CREATE TABLE dds.l_product_restaurant
(
    hk_product_restaurant_pk uuid                        NOT NULL PRIMARY KEY,
    h_product_pk             uuid                        NOT NULL,
    h_restaurant_pk          uuid                        NOT NULL,
    load_dt                  timestamp WITHOUT TIME ZONE NOT NULL,
    load_src                 varchar                     NOT NULL,

    FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk),
    FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant (h_restaurant_pk)
);


DROP TABLE IF EXISTS dds.l_product_category;
CREATE TABLE dds.l_product_category
(
    hk_product_category_pk uuid                        NOT NULL PRIMARY KEY,
    h_product_pk           uuid                        NOT NULL,
    h_category_pk          uuid                        NOT NULL,
    load_dt                timestamp WITHOUT TIME ZONE NOT NULL,
    load_src               varchar                     NOT NULL,

    FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk),
    FOREIGN KEY (h_category_pk) REFERENCES dds.h_category (h_category_pk)
);

DROP TABLE IF EXISTS dds.l_order_user;
CREATE TABLE dds.l_order_user
(
    hk_order_user_pk uuid                        NOT NULL PRIMARY KEY,
    h_order_pk       uuid                        NOT NULL,
    h_user_pk        uuid                        NOT NULL,
    load_dt          timestamp WITHOUT TIME ZONE NOT NULL,
    load_src         varchar                     NOT NULL,

    FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk),
    FOREIGN KEY (h_user_pk) REFERENCES dds.h_user (h_user_pk)
);



DROP TABLE IF EXISTS dds.s_user_names;
CREATE TABLE dds.s_user_names
(
    h_user_pk uuid                        NOT NULL PRIMARY KEY,
    username  varchar                     NOT NULL,
    load_dt   timestamp WITHOUT TIME ZONE NOT NULL,
    load_src  varchar                     NOT NULL,

    FOREIGN KEY (h_user_pk) REFERENCES dds.h_user (h_user_pk)
);



DROP TABLE IF EXISTS dds.s_product_names;
CREATE TABLE dds.s_product_names
(
    h_product_pk uuid                        NOT NULL PRIMARY KEY,
    name         varchar                     NOT NULL,
    load_dt      timestamp WITHOUT TIME ZONE NOT NULL,
    load_src     varchar                     NOT NULL,

    FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk)
);


DROP TABLE IF EXISTS dds.s_restaurant_names;
CREATE TABLE dds.s_restaurant_names
(
    h_restaurant_pk uuid                        NOT NULL PRIMARY KEY,
    name            varchar                     NOT NULL,
    load_dt         timestamp WITHOUT TIME ZONE NOT NULL,
    load_src        varchar                     NOT NULL,

    FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant (h_restaurant_pk)
);



DROP TABLE IF EXISTS dds.s_order_cost;
CREATE TABLE dds.s_order_cost
(
    h_order_pk uuid                        NOT NULL PRIMARY KEY,
    cost       decimal(19, 5)              NOT NULL,
    payment    decimal(19, 5)              NOT NULL,
    load_dt    timestamp WITHOUT TIME ZONE NOT NULL,
    load_src   varchar                     NOT NULL,

    CHECK ( cost >= 0 ),
    CHECK ( payment >= 0 ),

    FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk)
);


DROP TABLE IF EXISTS dds.s_order_status;
CREATE TABLE dds.s_order_status
(
    h_order_pk uuid                        NOT NULL PRIMARY KEY,
    status     varchar                     NOT NULL,
    load_dt    timestamp WITHOUT TIME ZONE NOT NULL,
    load_src   varchar                     NOT NULL,

    FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk)
);

