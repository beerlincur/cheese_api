CREATE TABLE "users" (
	"id" serial NOT NULL,
	"name" character varying(32) NOT NULL,
	"contacts" character varying(255) NOT NULL,
	"login" character varying(255) NOT NULL,
	"password" character varying(255) NOT NULL,
	CONSTRAINT "users_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "users_roles" (
	"user_id" integer NOT NULL,
	"is_admin" BOOLEAN NOT NULL DEFAULT 'false',
	"is_driver" BOOLEAN NOT NULL DEFAULT 'false',
	"is_operator" BOOLEAN NOT NULL DEFAULT 'false',
	"is_superuser" BOOLEAN NOT NULL DEFAULT 'false'
) WITH (
  OIDS=FALSE
);



CREATE TABLE "clients" (
	"id" serial NOT NULL,
	"name" character varying(32) NOT NULL,
	"entity" character varying(255) NOT NULL,
	"address" character varying(255) NOT NULL,
	"address_comments" TEXT NOT NULL,
	"network" character varying(255) NOT NULL,
	"payment" character varying(32) NOT NULL,
	"default_provider" integer NOT NULL,
	"recoil" float(2) NOT NULL,
	"comments" TEXT NOT NULL,
	CONSTRAINT "clients_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "providers" (
	"id" serial NOT NULL,
	"name" character varying(32) NOT NULL,
	"contacts" character varying(255) NOT NULL,
	"comments" TEXT NOT NULL,
	CONSTRAINT "providers_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "clients_sales" (
	"id" serial NOT NULL,
	"delivery_time" TIMESTAMP NOT NULL,
	"client" integer NOT NULL,
	"provider" integer NOT NULL,
	"driver" integer NOT NULL,
	"paid" float(2) NOT NULL,
	"debt" float(2) NOT NULL,
	"comments" TEXT NOT NULL,
	"status" character varying(32) NOT NULL,
	CONSTRAINT "clients_sales_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "providers_purchases" (
	"id" serial NOT NULL,
	"delivery_time" TIMESTAMP NOT NULL,
	"provider" integer NOT NULL,
	"product" character varying(255) NOT NULL,
	"amount" integer NOT NULL,
	"weight" float(2) NOT NULL,
	"price_per_kilo" float(2) NOT NULL,
	"total_price" float(2) NOT NULL,
	"paid" float(2) NOT NULL,
	"debt" float(2) NOT NULL,
	"comments" TEXT NOT NULL,
	"status" character varying(32) NOT NULL,
	CONSTRAINT "providers_purchases_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "drivers_share" (
	"id" serial NOT NULL,
	"driver_id" integer NOT NULL,
	"purchase_id" integer NOT NULL,
	"amount" integer NOT NULL,
	"weight" float(2) NOT NULL,
	"price_per_kilo" float(2) NOT NULL,
	CONSTRAINT "drivers_share_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "history" (
	"id" serial NOT NULL,
	"sale_id" integer NOT NULL,
	"share_id" integer NOT NULL,
	"driver_id" integer NOT NULL,
	"amount" integer NOT NULL,
	"weight" float(2) NOT NULL,
	"price_per_kilo" float(2) NOT NULL,
	"total_price" float(2) NOT NULL,
	CONSTRAINT "history_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "clients_work_hours" (
	"id" serial NOT NULL,
	"client_id" integer NOT NULL,
	"monday" character varying(64) NOT NULL,
	"tuesday" character varying(64) NOT NULL,
	"wednesday" character varying(64) NOT NULL,
	"thursday" character varying(64) NOT NULL,
	"friday" character varying(64) NOT NULL,
	"saturday" character varying(64) NOT NULL,
	"sunday" character varying(64) NOT NULL,
	CONSTRAINT "clients_work_hours_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);

CREATE TABLE "clients_future_sales" (
	"id" serial NOT NULL,
	"client" integer NOT NULL,
	"product" character varying(255) NOT NULL,
	"amount" integer NOT NULL,
	"order_time" TIMESTAMP NOT NULL,
	"delivery_time" TIMESTAMP NOT NULL,
	"comments" TEXT NOT NULL,
	CONSTRAINT "clients_future_sales_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



ALTER TABLE "users_roles" ADD CONSTRAINT "users_roles_fk0" FOREIGN KEY ("user_id") REFERENCES "users"("id");

ALTER TABLE "clients" ADD CONSTRAINT "clients_fk0" FOREIGN KEY ("default_provider") REFERENCES "providers"("id");


ALTER TABLE "clients_sales" ADD CONSTRAINT "clients_sales_fk0" FOREIGN KEY ("client") REFERENCES "clients"("id");
ALTER TABLE "clients_sales" ADD CONSTRAINT "clients_sales_fk1" FOREIGN KEY ("provider") REFERENCES "providers"("id");
ALTER TABLE "clients_sales" ADD CONSTRAINT "clients_sales_fk2" FOREIGN KEY ("driver") REFERENCES "users"("id");

ALTER TABLE "providers_purchases" ADD CONSTRAINT "providers_purchases_fk0" FOREIGN KEY ("provider") REFERENCES "providers"("id");

ALTER TABLE "drivers_share" ADD CONSTRAINT "drivers_share_fk0" FOREIGN KEY ("driver_id") REFERENCES "users"("id");
ALTER TABLE "drivers_share" ADD CONSTRAINT "drivers_share_fk1" FOREIGN KEY ("purchase_id") REFERENCES "providers_purchases"("id");

ALTER TABLE "history" ADD CONSTRAINT "history_fk0" FOREIGN KEY ("sale_id") REFERENCES "clients_sales"("id");
ALTER TABLE "history" ADD CONSTRAINT "history_fk1" FOREIGN KEY ("share_id") REFERENCES "drivers_share"("id");
ALTER TABLE "history" ADD CONSTRAINT "history_fk2" FOREIGN KEY ("driver_id") REFERENCES "users"("id");

ALTER TABLE "clients_work_hours" ADD CONSTRAINT "clients_work_hours_fk0" FOREIGN KEY ("client_id") REFERENCES "clients"("id");

ALTER TABLE "clients_future_sales" ADD CONSTRAINT "clients_future_sales_fk0" FOREIGN KEY ("client") REFERENCES "clients"("id");