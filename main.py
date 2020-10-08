import logging
from time import gmtime, localtime, strftime
from typing import Dict, List, Optional, Any

import psycopg2
from psycopg2.sql import SQL, Identifier
from config_db import config_database
from fastapi import FastAPI


app = FastAPI() # uvicorn main:app --host 195.2.76.198 --port 80

root_logger= logging.getLogger()
root_logger.setLevel(logging.INFO)
handler = logging.FileHandler('cheese_api.log', 'w', 'utf-8')
formatter = logging.Formatter('%(levelname)s - %(message)s')
handler.setFormatter(formatter)
root_logger.addHandler(handler)


@app.get("/")
def hello_world():
    return {"hello": "world"}


# ========================================================================== CREATE
@app.post("/create_user/")
def create_new_user(name: str, 
                    contacts: str, 
                    login: str, 
                    password: str, 
                    is_admin: bool, 
                    is_driver: bool, 
                    is_operator: bool, 
                    is_superuser: bool,
                    comments: Optional[str] = ""):
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        create_user_sql = "insert into users ( name,\
                                               contacts,\
                                               login,\
                                               password) values (%s, %s, %s, %s) returning id;"
        cur.execute(create_user_sql, (name, contacts, login, password))

        new_user_id = cur.fetchone()[0]
        
        create_user_in_users_roles = "insert into users_roles values (%s, %s, %s, %s, %s);"
        cur.execute(create_user_in_users_roles, (new_user_id, is_admin, is_driver, is_operator, is_superuser))
        cur.close()
        
        conn.commit()
        
        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW USER | {name} | created successfully")

        return {
            "new_user": {
                "id": new_user_id,
                "name": name,
                "contacts": contacts,
                "login": login,
                "password": password,
                "roles": {
                    "is_admin": is_admin,
                    "is_driver": is_driver,
                    "is_operator": is_operator,
                    "is_superuser": is_superuser
                }
            } 
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {
            "error": str(error)
        }
    finally:
        if conn is not None:
            conn.close()


@app.post("/create_provider/")
def create_new_provider(name: str,
                        contacts: str,
                        comments: Optional[str] = ""):
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        create_provider_sql = "insert into providers ( name,\
                                                       contacts,\
                                                       comments) values (%s, %s, %s) returning id;"
                                                
        cur.execute(create_provider_sql, (name,
                                          contacts, 
                                          comments))
        
        new_provider_id = cur.fetchone()[0]
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW PROVIDER | {name} | created successfully")

        return {
            "new_provider": {
                "id": new_provider_id,
                "name": name, 
                "contacts": contacts, 
                "comments": comments
            } 
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()



@app.post("/create_client/")
def create_new_client(name: str, 
                      entity: str, 
                      address: str,
                      payment: str, 
                      default_provider_id: int, 
                      recoil: float,
                      monday: str,
                      tuesday: str,
                      wednesday: str,
                      thursday: str,
                      friday: str,
                      saturday: str,
                      sunday: str,
                      network: Optional[str] = "",
                      address_comments: Optional[str] = "",
                      comments: Optional[str] = ""):
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        create_client_sql = "insert into clients ( name,\
                                                   entity,\
                                                   address,\
                                                   address_comments,\
                                                   network,\
                                                   payment,\
                                                   default_provider,\
                                                   recoil,\
                                                   comments) values (%s, %s, %s, %s, %s, %s, %s, %s, %s) returning id;"
                                                
        cur.execute(create_client_sql, (name,
                                        entity, 
                                        address, 
                                        address_comments, 
                                        network, 
                                        payment, 
                                        default_provider_id, 
                                        recoil, 
                                        comments))

        new_client_id = cur.fetchone()[0]
        
        create_clients_work_hours = "insert into clients_work_hours ( client_id,\
                                                                      monday,\
                                                                      tuesday,\
                                                                      wednesday,\
                                                                      thursday,\
                                                                      friday,\
                                                                      saturday,\
                                                                      sunday) values (%s, %s, %s, %s, %s, %s, %s, %s);"
        
        cur.execute(create_clients_work_hours, (new_client_id, 
                                                monday,
                                                tuesday,
                                                wednesday,
                                                thursday,
                                                friday,
                                                saturday,
                                                sunday))
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW CLIENT | {name} | created successfully")

        return {
            "new_client": {
                "id": new_client_id,
                "name": name, 
                "entity": entity, 
                "address": address, 
                "address_comments": address_comments, 
                "network": network, 
                "payment": payment, 
                "default_provider": default_provider_id, 
                "recoil": recoil,
                "comments": comments,
                "work_hours": {
                    "monday": monday,
                    "tuesday": tuesday,
                    "wednesday": wednesday,
                    "thursday": thursday,
                    "friday": friday,
                    "saturday": saturday,
                    "sunday": sunday 
                }
            } 
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.post("/create_purchase/")
def create_new_purchase( delivery_time: str, 
                         provider_id: str, 
                         product: str, 
                         amount: int, 
                         weight: float, 
                         price_per_kilo: float,
                         status: str,
                         total_price: Optional[float] = None,
                         paid: Optional[float] = None,
                         debt: Optional[float] = None,
                         comments: Optional[str] = ""
                         ):
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        total_price = weight * price_per_kilo if not total_price else total_price
        paid = 0 if not paid else paid
        debt = total_price if not debt else debt

        create_purchase_sql = "insert into providers_purchases ( delivery_time,\
                                                                 provider,\
                                                                 product,\
                                                                 amount,\
                                                                 weight,\
                                                                 price_per_kilo,\
                                                                 total_price,\
                                                                 paid,\
                                                                 debt,\
                                                                 comments,\
                                                                 status )\
                                                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning id;"
                                                
        cur.execute(create_purchase_sql, ( delivery_time, # 1999-01-08 04:05:06
                                           provider_id, 
                                           product, 
                                           amount, 
                                           weight, 
                                           price_per_kilo, 
                                           total_price, 
                                           paid,
                                           debt,
                                           comments,
                                           status ))
        
        new_purchase_id = cur.fetchone()[0]

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW PURCHASE | prod: {product} | prov: {provider_id} created successfully")

        return {
            "new_purchase": {
                "id": new_purchase_id,
                "delivery_time": delivery_time,
                "provider": provider_id,
                "product": product,
                "amount": amount,
                "weight": weight,
                "price_per_kilo": price_per_kilo,
                "total_price": total_price,
                "paid": paid,
                "debt": debt,
                "comments": comments,
                "status": status
            } 
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.post("/create_sale/")
def create_new_sale( delivery_time: str,
                     client_id: str,
                     provider_id: str,
                     driver_id: str, 
                     status: str,
                     paid: float,
                     debt: float,
                     comments: Optional[str] = ""
                     ):
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        create_sale_sql = "insert into clients_sales ( delivery_time,\
                                                       client,\
                                                       provider,\
                                                       driver,\
                                                       paid,\
                                                       debt,\
                                                       comments,\
                                                       status ) values (%s, %s, %s, %s, %s, %s, %s, %s) returning id;"
                                                
        cur.execute(create_sale_sql, ( delivery_time,
                                       client_id,
                                       provider_id,
                                       driver_id,
                                       paid,
                                       debt,
                                       comments,
                                       status ))

        new_sale_id = cur.fetchone()[0]

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW SALE | c: {client_id} | p: {provider_id} | d: {driver_id} created successfully")

        return {
            "new_sale": {
                "id": new_sale_id,
                "delivery_time": delivery_time,
                "client": client_id,
                "provider": provider_id,
                "driver": driver_id,
                "paid": paid,
                "debt": debt,
                "comments": comments,
                "status": status
            } 
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.post("/create_share/")
def create_new_share(driver_id: int, 
                     purchase_id: int,
                     amount: int,
                     weight: float,
                     price_per_kilo: float,
                     status: str
                    ):
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        create_share_sql = "insert into drivers_share ( driver_id,\
                                                        purchase_id,\
                                                        amount,\
                                                        weight,\
                                                        price_per_kilo,\
                                                        status\
                                                        ) values (%s, %s, %s, %s, %s, %s) returning id;"
                                                
        cur.execute(create_share_sql, ( driver_id,
                                        purchase_id,
                                        amount,
                                        weight,
                                        price_per_kilo,
                                        status ))
        
        new_share_id = cur.fetchone()[0]

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW SHARE | d: {driver_id} | pur: {purchase_id} created successfully")

        return {
            "new_share": {
                "id": new_share_id,
                "driver_id": driver_id,
                "purchase_id": purchase_id,
                "amount": amount,
                "weight": weight,
                "price_per_kilo": price_per_kilo,
                "status": status
            } 
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.post("/create_story/")
def create_new_story(sale_id: int, 
                     share_id: int,
                     amount: int,
                     weight: float,
                     price_per_kilo: float,
                     total_price: Optional[float] = 0
                    ):
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        total_price = weight * price_per_kilo if not total_price else total_price

        create_story_sql = "insert into history ( sale_id,\
                                                  share_id,\
                                                  amount,\
                                                  weight,\
                                                  price_per_kilo,\
                                                  total_price ) values (%s, %s, %s, %s, %s, %s) returning id;"
                                                
        cur.execute(create_story_sql, ( sale_id,
                                        share_id,
                                        amount,
                                        weight,
                                        price_per_kilo,
                                        total_price ))
        
        new_story_id = cur.fetchone()[0]

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW STORY | s: {sale_id} | sh: {share_id} created successfully")

        return {
            "new_story": {
                "id": new_story_id,
                "sale_id": sale_id,
                "share_id": share_id,
                "amount": amount,
                "weight": weight,
                "price_per_kilo": price_per_kilo,
                "total_price": total_price
            } 
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.post("/create_clients_future_sale/")
def create_clients_future_sale(client_id: int, 
                               product: str,
                               amount: int,
                               order_time: str,
                               delivery_time: str,
                               status: str,
                               comments: Optional[str] = ""
                               ):
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        create_future_sale_sql = "insert into clients_future_sales ( client,\
                                                                     product,\
                                                                     amount,\
                                                                     order_time,\
                                                                     delivery_time,\
                                                                     status,\
                                                                     comments) values (%s, %s, %s, %s, %s, %s, %s) returning id;"
                                                
        cur.execute(create_future_sale_sql, ( client_id,
                                              product,
                                              amount,
                                              order_time,
                                              delivery_time,
                                              status,
                                              comments))
        
        new_future_sale_id = cur.fetchone()[0]

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW FUTURE SALE | client: {client_id} | product: {product} created successfully")

        return {
            "new_future_sale": {
                "id": new_future_sale_id,
                "client_id": client_id,
                "product": product,
                "amount": amount,
                "order_time": order_time,
                "delivery_time": delivery_time,
                "status": status,
                "comments": comments
            } 
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()

@app.post("/create_product/")
def create_new_product(product_name: str):
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        create_product_sql = "insert into products (product_name) values (%s) returning id;"
                                                
        cur.execute(create_product_sql, (product_name,))

        new_product_id = cur.fetchone()[0]
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW PRODUCT | {product_name} | created successfully")

        return {
            "new_product": {
                "id": new_product_id,
                "product_name": product_name
            } 
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.post("/create_client_price/")
def create_new_client_price( product_name: str,
                             client_id: int, 
                             price: float ):
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        create_client_price_sql = "insert into clients_prices (product_name, client_id, price) values (%s, %s, %s) returning id;"
                                                
        cur.execute(create_client_price_sql, (product_name, client_id, price))

        new_client_price_id = cur.fetchone()[0]
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW CLIENT PRICE | {product_name} | {client_id} | created successfully")

        return {
            "new_client_price": {
                "id": new_client_price_id,
                "product_name": product_name,
                "client_id": client_id,
                "price": price
            } 
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()

# ========================================================================================= GET
@app.get("/get_all_users/")
def get_all_users():
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        get_all_users_sql = "select id,\
                                    name,\
                                    contacts,\
                                    login,\
                                    password from users;"

        cur.itersize = 200
                                                
        cur.execute(get_all_users_sql)

        users_json = {user[0]: { "name": user[1],
                                 "contacts": user[2],
                                 "login": user[3],
                                 "password": user[4] } for user in cur}
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL USERS successfully")
        
        return {
            "users": users_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_providers/")
def get_all_providers():
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        get_all_providers_sql = "select id,\
                                        name,\
                                        contacts,\
                                        comments from providers;"

        cur.itersize = 200
                                                
        cur.execute(get_all_providers_sql)

        providers_json = {provider[0]: { "name": provider[1],
                                         "contacts": provider[2],
                                         "comments": provider[3] } for provider in cur}
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL PROVIDERS successfully")
        
        return {
            "providers": providers_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_clients/")
def get_all_clients():
    conn = None
    try:
        params = config_database()

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        get_all_clients_sql = "select clients.id,\
                                      name,\
                                      entity,\
                                      address,\
                                      address_comments,\
                                      network,\
                                      payment,\
                                      default_provider,\
                                      recoil,\
                                      comments,\
                                      monday,\
                                      tuesday,\
                                      wednesday,\
                                      thursday,\
                                      friday,\
                                      saturday,\
                                      sunday from clients \
                                        left join clients_work_hours on clients.id = clients_work_hours.client_id;"

        cur.itersize = 200
                                                
        cur.execute(get_all_clients_sql)

        clients_json = {client[0]: { "name": client[1],
                                     "entity": client[2],
                                     "address": client[3],
                                     "address_comments": client[4],
                                     "network": client[5],
                                     "payment": client[6],
                                     "default_provider": client[7],
                                     "recoil": client[8],
                                     "comments": client[9],
                                     "work_hours": {
                                         "monday": client[10],
                                         "tuesday": client[11],
                                         "wednesday": client[12],
                                         "thursday": client[13],
                                         "friday": client[14],
                                         "saturday": client[15],
                                         "sunday": client[16] } } for client in cur}

        cur.close()

        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL CLIENTS successfully")

        return {
            "clients": clients_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_purchases/")
def get_all_purchases(provider_id: Optional[int] = None, product_name: Optional[str] = None, status: Optional[str] = None):
    conn = None
    try:
        params = config_database()

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        filter_str = ""

        is_already_one_filter = False

        if (provider_id is not None) or (product_name is not None) or (status is not None):
            filter_str = " where "
        
            if provider_id is not None:
                is_already_one_filter = True
                filter_str += r"providers.id = %s"
            
            if product_name is not None:
                if is_already_one_filter:
                    filter_str += " and "
                
                is_already_one_filter = True
                filter_str += r"product = %s"
            
            if status is not None:
                if is_already_one_filter:
                    filter_str += " and "
                
                is_already_one_filter = True
                filter_str += r"status = %s"

        get_all_purchases_sql = "select providers_purchases.id,\
                                      delivery_time,\
                                      providers.id,\
                                      providers.name,\
                                      providers.contacts,\
                                      providers.comments,\
                                      product,\
                                      amount,\
                                      weight,\
                                      price_per_kilo,\
                                      total_price,\
                                      paid,\
                                      debt,\
                                      providers_purchases.comments,\
                                      status from providers_purchases \
                                        left join providers on providers_purchases.provider = providers.id" + filter_str + ";"

        cur.itersize = 200

        parametrs_to_cur = []

        if provider_id is not None:
            parametrs_to_cur.append(provider_id)
        
        if product_name is not None:
            parametrs_to_cur.append(product_name)
        
        if status is not None:
            parametrs_to_cur.append(status)

        cur.execute(get_all_purchases_sql, tuple(parametrs_to_cur))           

        purchases_json = {purchase[0]: { "delivery_time": purchase[1],
                                         "provider": {
                                             "id": purchase[2],
                                             "name": purchase[3],
                                             "contacts": purchase[4],
                                             "comments": purchase[5]
                                         },
                                         "product": purchase[6],
                                         "amount": purchase[7],
                                         "weight": purchase[8],
                                         "price_per_kilo": purchase[9],
                                         "total_price": purchase[10],
                                         "paid": purchase[11],
                                         "debt": purchase[12],
                                         "comments": purchase[13],
                                         "status": purchase[14] } for purchase in cur}

        cur.close()

        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT PURCHASES successfully")

        return {
            "purchases": purchases_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_sales/")
def get_all_sales(driver_id: Optional[int] = None, client_id: Optional[int] = None, status: Optional[str] = None):
    conn = None
    try:
        params = config_database()

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        filter_str = ""

        is_already_one_filter = False

        if (driver_id is not None) or (client_id is not None) or (status is not None):
            filter_str = " where "
        
            if driver_id is not None:
                is_already_one_filter = True
                filter_str += r"s_driver.id = %s"
            
            if client_id is not None:
                if is_already_one_filter:
                    filter_str += " and "
                
                is_already_one_filter = True
                filter_str += r"s_client.id = %s"
            
            if status is not None:
                if is_already_one_filter:
                    filter_str += " and "
                
                is_already_one_filter = True
                filter_str += r"cs.status = %s"

        get_all_sales_sql = "select cs.id,\
                                    cs.delivery_time,\
                                    s_client.id,\
                                    s_client.name,\
                                    s_client.entity,\
                                    s_client.address,\
                                    s_client.address_comments,\
                                    s_client.network,\
                                    s_client.payment,\
                                    s_client.default_provider,\
                                    def_prov.id,\
                                    def_prov.name,\
                                    def_prov.contacts,\
                                    def_prov.comments,\
                                    s_client.recoil,\
                                    s_client.comments,\
                                    cwh.monday,\
                                    cwh.tuesday,\
                                    cwh.wednesday,\
                                    cwh.thursday,\
                                    cwh.friday,\
                                    cwh.saturday,\
                                    cwh.sunday,\
                                    s_provider.id,\
                                    s_provider.name,\
                                    s_provider.contacts,\
                                    s_provider.comments,\
                                    s_driver.id,\
                                    s_driver.name,\
                                    s_driver.contacts,\
                                    cs.paid,\
                                    cs.debt,\
                                    cs.comments,\
                                    cs.status from clients_sales cs\
                                    left join clients s_client on cs.client = s_client.id\
                                    left join providers s_provider on cs.provider = s_provider.id\
                                    left join users s_driver on cs.driver = s_driver.id\
                                    left join clients_work_hours cwh on cs.client = cwh.client_id\
                                    left join providers def_prov on s_client.default_provider = def_prov.id" + filter_str + ";"

        cur.itersize = 200

        parametrs_to_cur = []

        if driver_id is not None:
            parametrs_to_cur.append(driver_id)
        
        if client_id is not None:
            parametrs_to_cur.append(client_id)
        
        if status is not None:
            parametrs_to_cur.append(status)

        cur.execute(get_all_sales_sql, tuple(parametrs_to_cur))
        

        sales_json = {sale[0]: { 
            "delivery_time": sale[1],
            "client": {
                "id": sale[2],
                "name": sale[3],
                "entity": sale[4],
                "address": sale[5],
                "address_comments": sale[6],
                "network": sale[7],
                "payment": sale[8],
                "default_provider_id": sale[9],
                "default_provider": {
                    "id": sale[10],
                    "name": sale[11],
                    "contacts": sale[12],
                    "comments": sale[13]
                },
                "recoil": sale[14],
                "comments": sale[15],
                "work_hours": {
                    "monday": sale[16],
                    "tuesday": sale[17],
                    "wednesday": sale[18],
                    "thursday": sale[19],
                    "friday": sale[20],
                    "saturday": sale[21],
                    "sunday": sale[22]
                }
            },
            "provider": {
                "id": sale[23],
                "name": sale[24],
                "contacts": sale[25],
                "comments": sale[26]
            },
            "driver": {
                "id": sale[27],
                "name": sale[28],
                "contacts": sale[29]
            },
            "paid": sale[30],
            "debt": sale[31],
            "comments": sale[32],
            "status": sale[33]
        } for sale in cur}

        cur.close()

        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT SALES successfully")

        return {
            "sales": sales_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_shares/")
def get_all_shares(driver_id: Optional[int] = None, purchase_id: Optional[int] = None, status: Optional[str] = None):
    conn = None
    try:
        params = config_database()

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        filter_str = ""

        is_already_one_filter = False

        if (driver_id is not None) or (purchase_id is not None) or (status is not None):
            filter_str = " where "
        
            if driver_id is not None:
                is_already_one_filter = True
                filter_str += r"dr.id = %s"
            
            if purchase_id is not None:
                if is_already_one_filter:
                    filter_str += " and "
                
                is_already_one_filter = True
                filter_str += r"pp.id = %s"
            
            if status is not None:
                if is_already_one_filter:
                    filter_str += " and "
                
                is_already_one_filter = True
                filter_str += r"ds.status = %s"

        get_all_share_sql = "select ds.id,\
                                    dr.id,\
                                    dr.name,\
                                    dr.contacts,\
                                    pp.id,\
                                    pp.delivery_time,\
                                    pr.id,\
                                    pr.name,\
                                    pr.contacts,\
                                    pr.comments,\
                                    pp.product,\
                                    pp.amount,\
                                    pp.weight,\
                                    pp.price_per_kilo,\
                                    pp.total_price,\
                                    pp.paid,\
                                    pp.debt,\
                                    pp.comments,\
                                    pp.status,\
                                    ds.amount,\
                                    ds.weight,\
                                    ds.price_per_kilo,\
                                    ds.status from drivers_share ds\
                                    left join users dr on ds.driver_id = dr.id\
                                    left join providers_purchases pp on ds.purchase_id = pp.id\
                                    left join providers pr on pp.provider = pr.id" + filter_str + ";"

        cur.itersize = 200

        parametrs_to_cur = []

        if driver_id is not None:
            parametrs_to_cur.append(driver_id)
        
        if purchase_id is not None:
            parametrs_to_cur.append(purchase_id)
        
        if status is not None:
            parametrs_to_cur.append(status)

        cur.execute(get_all_share_sql, tuple(parametrs_to_cur))

        shares_json = {share[0]: { 
            "driver": {
                "id": share[1],
                "name": share[2],
                "contacts": share[3]
            },
            "purchase": {
                "id": share[4],
                "delivery_time": share[5],
                "provider": {
                    "id": share[6],
                    "name": share[7],
                    "contacts": share[8],
                    "comments": share[9]
                },
                "product": share[10],
                "amount": share[11],
                "weight": share[12],
                "price_per_kilo": share[13],
                "total_price": share[14],
                "paid": share[15],
                "debt": share[16],
                "comments": share[17],
                "status": share[18]
            },
            "amount": share[19],
            "weight": share[20],
            "price_per_kilo": share[21],
            "status": share[22]
        } for share in cur}

        cur.close()

        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT DRIVERS SHARES successfully")

        return {
            "shares": shares_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_history/")
def get_all_history(client_id: Optional[int] = None, driver_id: Optional[int] = None, provider_id: Optional[int] = None):
    conn = None
    try:
        params = config_database()

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        filter_str = ""

        is_already_one_filter = False

        if (client_id is not None) or (driver_id is not None) or (provider_id is not None):
            filter_str = " where "
        
            if client_id is not None:
                is_already_one_filter = True
                filter_str += r"s_client.id = %s"
            
            if driver_id is not None:
                if is_already_one_filter:
                    filter_str += " and "
                
                is_already_one_filter = True
                filter_str += r"dr.id = %s"
            
            if provider_id is not None:
                if is_already_one_filter:
                    filter_str += " and "
                
                is_already_one_filter = True
                filter_str += r"pr.id = %s"

        get_all_history_sql = "select h.id,\
                                    cs.id,\
                                    cs.delivery_time,\
                                    s_client.id,\
                                    s_client.name,\
                                    s_client.entity,\
                                    s_client.address,\
                                    s_client.address_comments,\
                                    s_client.network,\
                                    s_client.payment,\
                                    s_client.default_provider,\
                                    def_prov.id,\
                                    def_prov.name,\
                                    def_prov.contacts,\
                                    def_prov.comments,\
                                    s_client.recoil,\
                                    s_client.comments,\
                                    cwh.monday,\
                                    cwh.tuesday,\
                                    cwh.wednesday,\
                                    cwh.thursday,\
                                    cwh.friday,\
                                    cwh.saturday,\
                                    cwh.sunday,\
                                    s_provider.id,\
                                    s_provider.name,\
                                    s_provider.contacts,\
                                    s_provider.comments,\
                                    s_driver.id,\
                                    s_driver.name,\
                                    s_driver.contacts,\
                                    cs.paid,\
                                    cs.debt,\
                                    cs.comments,\
                                    cs.status,\
                                    ds.id,\
                                    dr.id,\
                                    dr.name,\
                                    dr.contacts,\
                                    pp.id,\
                                    pp.delivery_time,\
                                    pr.id,\
                                    pr.name,\
                                    pr.contacts,\
                                    pr.comments,\
                                    pp.product,\
                                    pp.amount,\
                                    pp.weight,\
                                    pp.price_per_kilo,\
                                    pp.total_price,\
                                    pp.paid,\
                                    pp.debt,\
                                    pp.comments,\
                                    pp.status,\
                                    ds.amount,\
                                    ds.weight,\
                                    ds.price_per_kilo,\
                                    ds.status,\
                                    h.amount,\
                                    h.weight,\
                                    h.price_per_kilo,\
                                    h.total_price from history h\
                                    left join clients_sales cs on h.sale_id = cs.id\
                                    left join clients s_client on cs.client = s_client.id\
                                    left join providers s_provider on cs.provider = s_provider.id\
                                    left join users s_driver on cs.driver = s_driver.id\
                                    left join clients_work_hours cwh on cs.client = cwh.client_id\
                                    left join providers def_prov on s_client.default_provider = def_prov.id\
                                    left join drivers_share ds on h.share_id = ds.id\
                                    left join users dr on ds.driver_id = dr.id\
                                    left join providers_purchases pp on ds.purchase_id = pp.id\
                                    left join providers pr on pp.provider = pr.id;"

        cur.itersize = 200

        parametrs_to_cur = []

        if client_id is not None:
            parametrs_to_cur.append(client_id)
        
        if driver_id is not None:
            parametrs_to_cur.append(driver_id)
        
        if provider_id is not None:
            parametrs_to_cur.append(provider_id)

        cur.execute(get_all_history_sql, tuple(parametrs_to_cur))

        history_json = {story[0]: { 
            "sale": {
                "id": story[1],
                "delivery_time": story[2],
                "client": {
                    "id": story[3],
                    "name": story[4],
                    "entity": story[5],
                    "address": story[6],
                    "address_comments": story[7],
                    "network": story[8],
                    "payment": story[9],
                    "default_provider_id": story[10],
                    "default_provider": {
                        "id": story[11],
                        "name": story[12],
                        "contacts": story[13],
                        "comments": story[14]
                    },
                    "recoil": story[15],
                    "comments": story[16],
                    "work_hours": {
                        "monday": story[17],
                        "tuesday": story[18],
                        "wednesday": story[19],
                        "thursday": story[20],
                        "friday": story[21],
                        "saturday": story[22],
                        "sunday": story[23]
                    }
                },
                "provider": {
                    "id": story[24],
                    "name": story[25],
                    "contacts": story[26],
                    "comments": story[27]
                },
                "driver": {
                    "id": story[28],
                    "name": story[29],
                    "contacts": story[30]
                },
                "paid": story[31],
                "debt": story[32],
                "comments": story[33],
                "status": story[34]
            },
            "share": {
                "id": story[35], 
                "driver": {
                    "id": story[36],
                    "name": story[37],
                    "contacts": story[38]
                },
                "purchase": {
                    "id": story[39],
                    "delivery_time": story[40],
                    "provider": {
                        "id": story[41],
                        "name": story[42],
                        "contacts": story[43],
                        "comments": story[44]
                    },
                    "product": story[45],
                    "amount": story[46],
                    "weight": story[47],
                    "price_per_kilo": story[48],
                    "total_price": story[49],
                    "paid": story[50],
                    "debt": story[51],
                    "comments": story[52],
                    "status": story[53]
                },
                "amount": story[54],
                "weight": story[55],
                "price_per_kilo": story[56],
                "status": story[57]
            },
            "amount": story[58],
            "weight": story[59],
            "price_per_kilo": story[60],
            "total_price": story[61]
        } for story in cur}

        cur.close()

        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT HISTORY successfully")

        return {
            "history": history_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_drivers_users/")
def get_all_drivers_users():
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        get_all_drivers_sql = "select id,\
                                      name from users\
                                      left join users_roles on\
                                      users.id = users_roles.user_id where is_driver='t';"

        cur.itersize = 200
                                                
        cur.execute(get_all_drivers_sql)

        drivers_json = {driver[0]: { "name": driver[1] } for driver in cur}
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL DRIVERS successfully")
        
        return {
            "drivers": drivers_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_admin_users/")
def get_all_admin_users():
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        get_all_admins_sql = "select id,\
                                     name from users\
                                     left join users_roles on\
                                     users.id = users_roles.user_id where is_admin='t';"

        cur.itersize = 200
                                                
        cur.execute(get_all_admins_sql)

        admins_json = {admin[0]: { "name": admin[1] } for admin in cur}
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL ADMINS successfully")
        
        return {
            "admins": admins_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_operator_users/")
def get_all_operator_users():
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        get_all_operators_sql = "select id,\
                                        name from users\
                                        left join users_roles on\
                                        users.id = users_roles.user_id where is_operator='t';"

        cur.itersize = 200
                                                
        cur.execute(get_all_operators_sql)

        operators_json = {operator[0]: { "name": operator[1] } for operator in cur}
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL OPERATORS successfully")
        
        return {
            "operators": operators_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_super_users/")
def get_all_super_users():
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        get_all_superusers_sql = "select id,\
                                         name from users\
                                         left join users_roles on\
                                         users.id = users_roles.user_id where is_operator='t';"

        cur.itersize = 200
                                                
        cur.execute(get_all_superusers_sql)

        superusers_json = {superuser[0]: { "name": superuser[1] } for superuser in cur}
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL SUPERUSERS successfully")
        
        return {
            "superusers": superusers_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_clients_names/")
def get_all_clients_names():
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        get_all_clients_names_sql = "select id,\
                                            name from clients;"

        cur.itersize = 200
                                                
        cur.execute(get_all_clients_names_sql)

        clients_json = {client[0]: { "name": client[1] } for client in cur}
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL CLIENTS NAMES successfully")
        
        return {
            "clients": clients_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_providers_names/")
def get_all_providers_names():
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        get_all_providers_names_sql = "select id,\
                                              name from providers;"

        cur.itersize = 200
                                                
        cur.execute(get_all_providers_names_sql)

        providers_json = {provider[0]: { "name": provider[1] } for provider in cur}
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL PROVIDERS NAMES successfully")
        
        return {
            "providers": providers_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_future_sales/")
def get_all_future_sales(client_id: Optional[int] = None, status: Optional[str] = None):
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        filter_str = ""

        is_already_one_filter = False

        if (client_id is not None) or (status is not None):
            filter_str = " where "
            
            if client_id is not None:
                is_already_one_filter = True
                filter_str += r"c.id = %s"
            
            if status is not None:
                if is_already_one_filter:
                    filter_str += " and "
                
                is_already_one_filter = True
                filter_str += r"cfs.status = %s"
        
        get_all_cfuture_sales_sql = "select cfs.id,\
                                            c.id,\
                                            c.name,\
                                            c.entity,\
                                            c.address,\
                                            c.address_comments,\
                                            c.network,\
                                            c.payment,\
                                            def_prov.id,\
                                            def_prov.name,\
                                            def_prov.contacts,\
                                            def_prov.comments,\
                                            c.recoil,\
                                            c.comments,\
                                            cwh.monday,\
                                            cwh.tuesday,\
                                            cwh.wednesday,\
                                            cwh.thursday,\
                                            cwh.friday,\
                                            cwh.saturday,\
                                            cwh.sunday,\
                                            cfs.product,\
                                            cfs.amount,\
                                            cfs.order_time,\
                                            cfs.delivery_time,\
                                            cfs.status,\
                                            cfs.comments from clients_future_sales cfs\
                                            left join clients c on cfs.client = c.id\
                                            left join clients_work_hours cwh on cfs.client = cwh.client_id\
                                            left join providers def_prov on c.default_provider = def_prov.id" + filter_str + ";"

        cur.itersize = 200
                                                
        parametrs_to_cur = []
    
        if client_id is not None:
            parametrs_to_cur.append(client_id)
        
        if status is not None:
            parametrs_to_cur.append(status)

        cur.execute(get_all_cfuture_sales_sql, tuple(parametrs_to_cur))

        sales_json = {sale[0]: { 
            "client": {
                "id": sale[1],
                "name": sale[2],
                "entity": sale[3],
                "address": sale[4],
                "address_comments": sale[5],
                "network": sale[6],
                "payment": sale[7],
                "default_provider": {
                    "id": sale[8],
                    "name": sale[9],
                    "contacts": sale[10],
                    "comments": sale[11]
                },
                "recoil": sale[12],
                "comments": sale[13],
                "work_hours": {
                    "monday": sale[14],
                    "tuesday": sale[15],
                    "wednesday": sale[16],
                    "thursday": sale[17],
                    "friday": sale[18],
                    "saturday": sale[19],
                    "sunday": sale[20]
                }
            },
            "product": sale[21],
            "amount": sale[22],
            "order_time": sale[23],
            "delivery_time": sale[24],
            "status": sale[25],
            "comments": sale[26]
        } for sale in cur}
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT FUTURE SALES successfully")
        
        return {
            "future_sales": sales_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_products/")
def get_all_products():
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        get_all_products_sql = "select id, product_name from products;"

        cur.itersize = 200
                                                
        cur.execute(get_all_products_sql)

        products_json = {product[0]: { "product_name": product[1] } for product in cur}
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL PRODUCTS successfully")
        
        return {
            "products": products_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_all_clients_prices/")
def get_all_clients_prices(client_id: Optional[int] = None, product_name: Optional[str] = None):
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        filter_str = ""

        is_already_one_filter = False

        if (client_id is not None) or (product_name is not None):
            filter_str = " where "
            
            if client_id is not None:
                is_already_one_filter = True
                filter_str += r"s_client.id = %s"
            
            if product_name is not None:
                if is_already_one_filter:
                    filter_str += " and "
                
                is_already_one_filter = True
                filter_str += r"cp.product_name = %s"
        
        get_all_clients_prices_sql = "select cp.id, \
                                             cp.product_name,\
                                             s_client.id,\
                                             s_client.name,\
                                             s_client.entity,\
                                             s_client.address,\
                                             s_client.address_comments,\
                                             s_client.network,\
                                             s_client.payment,\
                                             s_client.default_provider,\
                                             def_prov.id,\
                                             def_prov.name,\
                                             def_prov.contacts,\
                                             def_prov.comments,\
                                             s_client.recoil,\
                                             s_client.comments,\
                                             cwh.monday,\
                                             cwh.tuesday,\
                                             cwh.wednesday,\
                                             cwh.thursday,\
                                             cwh.friday,\
                                             cwh.saturday,\
                                             cwh.sunday,\
                                             cp.price from clients_prices cp\
                                             left join clients s_client on cp.client_id = s_client.id\
                                             left join clients_work_hours cwh on cp.client_id = cwh.client_id\
                                             left join providers def_prov on s_client.default_provider = def_prov.id" + filter_str + ";"

        cur.itersize = 200

        parametrs_to_cur = []
    
        if client_id is not None:
            parametrs_to_cur.append(client_id)
        
        if product_name is not None:
            parametrs_to_cur.append(product_name)

        cur.execute(get_all_clients_prices_sql, tuple(parametrs_to_cur))

        clients_prices_json = {client_price[0]: { "product_name": client_price[1],
                                                  "client": {
                                                        "id": client_price[2],
                                                        "name": client_price[3],
                                                        "entity": client_price[4],
                                                        "address": client_price[5],
                                                        "address_comments": client_price[6],
                                                        "network": client_price[7],
                                                        "payment": client_price[8],
                                                        "default_provider_id": client_price[9],
                                                        "default_provider": {
                                                            "id": client_price[10],
                                                            "name": client_price[11],
                                                            "contacts": client_price[12],
                                                            "comments": client_price[13]
                                                        },
                                                        "recoil": client_price[14],
                                                        "comments": client_price[15],
                                                        "work_hours": {
                                                            "monday": client_price[16],
                                                            "tuesday": client_price[17],
                                                            "wednesday": client_price[18],
                                                            "thursday": client_price[19],
                                                            "friday": client_price[20],
                                                            "saturday": client_price[21],
                                                            "sunday": client_price[22]
                                                        }
                                                  },
                                                  "price": client_price[23] } for client_price in cur}
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT CLIENTS PRICES successfully")
        
        return {
            "clients_prices": clients_prices_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


# ========================================================================== UPDATE
@app.put("/update_users_cell/")
def update_users_cell(user_id: int,
                      column: str,
                      new_value: Any):
    conn = None
    try:
        if column == 'id':
            return {
                "error": "You cannot modify 'id' column"
            }
        
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        update_user_cell_sql = \
            SQL("update users set {} = %s where id = %s returning *;").format(Identifier(column))
                                                
        cur.execute(update_user_cell_sql, (new_value, user_id))

        updated_user_tuple = cur.fetchone()

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---UPDATED USER {user_id} | column {column} | new value {new_value}")
        
        return {
            "updated_user": {
                updated_user_tuple[0]: {
                    "name": updated_user_tuple[1],
                    "contacts": updated_user_tuple[2],
                    "login": updated_user_tuple[3],
                    "password": updated_user_tuple[4]
                }
            }
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.put("/update_users_roles_cell/")
def update_users_roles_cell(user_id: int,
                            role: str,
                            new_value: bool):
    conn = None
    try:
        if role == 'user_id':
            return {
                "error": "You cannot modify 'user_id' column"
            }

        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        update_user_role_cell_sql = \
            SQL("update users_roles set {} = %s where user_id = %s returning *;").format(Identifier(role))
                                                
        cur.execute(update_user_role_cell_sql, (new_value, user_id))

        updated_user_role_tuple = cur.fetchone()

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---UPDATED USER ROLES {user_id} | column {role} | new value {new_value}")
        
        return {
            "updated_user_roles": {
                updated_user_role_tuple[0]: {
                    "is_admin": updated_user_role_tuple[1],
                    "is_driver": updated_user_role_tuple[2],
                    "is_operator": updated_user_role_tuple[3],
                    "is_superuser": updated_user_role_tuple[4]
                }
            }
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.put("/update_providers_cell/")
def update_providers_cell(provider_id: int,
                          column: str,
                          new_value: Any):
    conn = None
    try:
        if column == 'id':
            return {
                "error": "You cannot modify 'id' column"
            }

        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        update_provider_cell_sql = \
            SQL("update providers set {} = %s where id = %s returning *;").format(Identifier(column))
                                                
        cur.execute(update_provider_cell_sql, (new_value, provider_id))

        updated_provider_tuple = cur.fetchone()

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---UPDATED PROVIDER {provider_id} | column {column} | new value {new_value}")
        
        return {
            "updated_provider": {
                updated_provider_tuple[0]: {
                    "name": updated_provider_tuple[1],
                    "contacts": updated_provider_tuple[2],
                    "comments": updated_provider_tuple[3]
                }
            }
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.put("/update_clients_cell/")
def update_clients_cell(client_id: int,
                        column: str,
                        new_value: Any):
    conn = None
    try:
        if column == 'id':
            return {
                "error": "You cannot modify 'id' column"
            }

        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        update_client_cell_sql = \
            SQL("update clients set {} = %s where id = %s returning *;").format(Identifier(column))
                                                
        cur.execute(update_client_cell_sql, (new_value, client_id))

        updated_client_tuple = cur.fetchone()

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---UPDATED CLIENT {client_id} | column {column} | new value {new_value}")
        
        return {
            "updated_client": {
                updated_client_tuple[0]: {
                    "name": updated_client_tuple[1],
                    "entity": updated_client_tuple[2],
                    "address": updated_client_tuple[3],
                    "address_comments": updated_client_tuple[4],
                    "network": updated_client_tuple[5],
                    "payment": updated_client_tuple[6],
                    "default_provider": updated_client_tuple[7],
                    "recoil": updated_client_tuple[8],
                    "comments": updated_client_tuple[9]
                }
            }
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.put("/update_clients_work_hours_cell/")
def update_clients_work_hours_cell(client_id: int,
                                   weekday: str,
                                   new_value: Any):
    conn = None
    try:
        if weekday == 'id' or weekday == 'client_id':
            return {
                "error": "You cannot modify 'id' or 'client_id' column"
            }

        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        update_client_wh_cell_sql = \
            SQL("update clients_work_hours set {} = %s where client_id = %s returning *;").format(Identifier(weekday))
                                                
        cur.execute(update_client_wh_cell_sql, (new_value, client_id))

        updated_client_wh_tuple = cur.fetchone()

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---UPDATED CLIENT WORK HOURS {client_id} | weekday {weekday} | new value {new_value}")
        
        return {
            "updated_client_work_hours": {
                updated_client_wh_tuple[0]: {
                    "monday": updated_client_wh_tuple[1],
                    "tuesday": updated_client_wh_tuple[2],
                    "wednesday": updated_client_wh_tuple[3],
                    "thursday": updated_client_wh_tuple[4],
                    "friday": updated_client_wh_tuple[5],
                    "saturday": updated_client_wh_tuple[6],
                    "sunday": updated_client_wh_tuple[7]
                }
            }
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()
    

@app.put("/update_providers_purchases_cell/")
def update_providers_purchases_cell(purchase_id: int,
                                    column: str,
                                    new_value: Any):
    conn = None
    try:
        if column == 'id':
            return {
                "error": "You cannot modify 'id' column"
            }

        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        update_pp_cell_sql = \
            SQL("update providers_purchases set {} = %s where id = %s returning *;").format(Identifier(column))
                                                
        cur.execute(update_pp_cell_sql, (new_value, purchase_id))

        updated_pp_tuple = cur.fetchone()

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---UPDATED PROVIDER PURCHASE {purchase_id} | column {column} | new value {new_value}")
        
        return {
            "updated_provider_purchase": {
                updated_pp_tuple[0]: {
                    "delivery_time": updated_pp_tuple[1],
                    "provider": updated_pp_tuple[2],
                    "product": updated_pp_tuple[3],
                    "amount": updated_pp_tuple[4],
                    "weight": updated_pp_tuple[5],
                    "price_per_kilo": updated_pp_tuple[6],
                    "total_price": updated_pp_tuple[7],
                    "paid": updated_pp_tuple[8],
                    "debt": updated_pp_tuple[9],
                    "comments": updated_pp_tuple[10],
                    "status": updated_pp_tuple[11]
                }
            }
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.put("/update_clients_sales_cell/")
def update_clients_sales_cell(sale_id: int,
                              column: str,
                              new_value: Any):
    conn = None
    try:
        if column == 'id':
            return {
                "error": "You cannot modify 'id' column"
            }

        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        update_sale_cell_sql = \
            SQL("update clients_sales set {} = %s where id = %s returning *;").format(Identifier(column))
                                                
        cur.execute(update_sale_cell_sql, (new_value, sale_id))

        updated_sale_tuple = cur.fetchone()

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---UPDATED CLIENT SALE {sale_id} | column {column} | new value {new_value}")
        
        return {
            "updated_client_sale": {
                updated_sale_tuple[0]: {
                    "delivery_time": updated_sale_tuple[1],
                    "client": updated_sale_tuple[2],
                    "provider": updated_sale_tuple[3],
                    "driver": updated_sale_tuple[4],
                    "paid": updated_sale_tuple[5],
                    "debt": updated_sale_tuple[6],
                    "comments": updated_sale_tuple[7],
                    "status": updated_sale_tuple[8]
                }
            }
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.put("/update_drivers_share_cell/")
def update_drivers_share_cell(share_id: int,
                              column: str,
                              new_value: Any):
    conn = None
    try:
        if column == 'id':
            return {
                "error": "You cannot modify 'id' column"
            }

        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        update_share_cell_sql = \
            SQL("update drivers_share set {} = %s where id = %s returning *;").format(Identifier(column))
                                                
        cur.execute(update_share_cell_sql, (new_value, share_id))

        updated_share_tuple = cur.fetchone()

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---UPDATED DRIVER SHARE {share_id} | column {column} | new value {new_value}")
        
        return {
            "updated_driver_share": {
                updated_share_tuple[0]: {
                    "driver_id": updated_share_tuple[1],
                    "purchase_id": updated_share_tuple[2],
                    "amount": updated_share_tuple[3],
                    "weight": updated_share_tuple[4],
                    "price_per_kilo": updated_share_tuple[5],
                    "status": updated_share_tuple[6]
                }
            }
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.put("/update_history_cell/")
def update_history_cell(story_id: int,
                        column: str,
                        new_value: Any):
    conn = None
    try:
        if column == 'id':
            return {
                "error": "You cannot modify 'id' column"
            }

        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        update_story_cell_sql = \
            SQL("update history set {} = %s where id = %s returning *;").format(Identifier(column))
                                                
        cur.execute(update_story_cell_sql, (new_value, story_id))

        updated_story_tuple = cur.fetchone()

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---UPDATED HISTORY {story_id} | column {column} | new value {new_value}")
        
        return {
            "updated_story": {
                updated_story_tuple[0]: {
                    "sale_id": updated_story_tuple[1],
                    "share_id": updated_story_tuple[2],
                    "amount": updated_story_tuple[3],
                    "weight": updated_story_tuple[4],
                    "price_per_kilo": updated_story_tuple[5],
                    "total_price": updated_story_tuple[6]
                }
            }
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.put("/update_clients_future_sales_cell/")
def update_clients_future_sales_cell(sale_id: int,
                                     column: str,
                                     new_value: Any):
    conn = None
    try:
        if column == 'id':
            return {
                "error": "You cannot modify 'id' column"
            }

        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        update_clients_future_sales_cell_sql = \
            SQL("update clients_future_sales set {} = %s where id = %s returning *;").format(Identifier(column))
                                                
        cur.execute(update_clients_future_sales_cell_sql, (new_value, sale_id))

        updated_clients_future_sales_tuple = cur.fetchone()

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---UPDATED FUTURE CLIENT SALE {sale_id} | column {column} | new value {new_value}")
        
        return {
            "updated_future_client_sale": {
                updated_clients_future_sales_tuple[0]: {
                    "client": updated_clients_future_sales_tuple[1],
                    "product": updated_clients_future_sales_tuple[2],
                    "amount": updated_clients_future_sales_tuple[3],
                    "order_time": updated_clients_future_sales_tuple[4],
                    "delivery_time": updated_clients_future_sales_tuple[5],
                    "status": updated_clients_future_sales_tuple[6],
                    "comments": updated_clients_future_sales_tuple[7]
                }
            }
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.put("/update_products_cell/")
def update_products_cell( product_id: int,
                          column: str,
                          new_value: Any ):
    conn = None
    try:
        if column == 'id':
            return {
                "error": "You cannot modify 'id' column"
            }

        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        update_product_cell_sql = \
            SQL("update products set {} = %s where id = %s returning *;").format(Identifier(column))
                                                
        cur.execute(update_product_cell_sql, (new_value, product_id))

        updated_product_tuple = cur.fetchone()

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---UPDATED PRODUCT {product_id} | column {column} | new value {new_value}")
        
        return {
            "updated_product": {
                updated_product_tuple[0]: {
                    "product_name": updated_product_tuple[1]
                }
            }
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.put("/update_clients_prices_cell/")
def update_clients_prices_cell( client_price_id: int,
                                column: str,
                                new_value: Any ):
    conn = None
    try:
        if column == 'id':
            return {
                "error": "You cannot modify 'id' column"
            }

        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        update_client_price_cell_sql = \
            SQL("update clients_prices set {} = %s where id = %s returning *;").format(Identifier(column))
                                                
        cur.execute(update_client_price_cell_sql, (new_value, client_price_id))

        updated_client_price_tuple = cur.fetchone()

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---UPDATED CLIENT PRICE {client_price_id} | column {column} | new value {new_value}")
        
        return {
            "updated_client_price": {
                updated_client_price_tuple[0]: {
                    "product_name": updated_client_price_tuple[1],
                    "client_id": updated_client_price_tuple[2],
                    "price": updated_client_price_tuple[3]
                }
            }
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


# ========================================================================== UPDATE CELL USERS
@app.put("/update_user_name/")
def update_user_name(user_id: int, name: str):
    return update_users_cell(user_id, 'name', name)

@app.put("/update_user_contacts/")
def update_user_contacts(user_id: int, contacts: str):
    return update_users_cell(user_id, 'contacts', contacts)

@app.put("/update_user_login/")
def update_user_login(user_id: int, login: str):
    return update_users_cell(user_id, 'login', login)

@app.put("/update_user_password/")
def update_user_password(user_id: int, password: str):
    return update_users_cell(user_id, 'password', password)

# ========================================================================== UPDATE CELL USERS_ROLES
@app.put("/update_user_role_is_admin/")
def update_user_role_is_admin(user_id: int, is_admin: bool):
    return update_users_roles_cell(user_id, 'is_admin', is_admin)

@app.put("/update_user_role_is_driver/")
def update_user_role_is_driver(user_id: int, is_driver: bool):
    return update_users_roles_cell(user_id, 'is_driver', is_driver)

@app.put("/update_user_role_is_operator/")
def update_user_role_is_operator(user_id: int, is_operator: bool):
    return update_users_roles_cell(user_id, 'is_operator', is_operator)

@app.put("/update_user_role_is_superuser/")
def update_user_role_is_superuser(user_id: int, is_superuser: bool):
    return update_users_roles_cell(user_id, 'is_superuser', is_superuser)

# ========================================================================== UPDATE CELL PROVIDERS
@app.put("/update_provider_name/")
def update_provider_name(provider_id: int, name: str):
    return update_providers_cell(provider_id, 'name', name)

@app.put("/update_provider_contacts/")
def update_provider_contacts(provider_id: int, contacts: str):
    return update_providers_cell(provider_id, 'contacts', contacts)

@app.put("/update_provider_comments/")
def update_provider_comments(provider_id: int, comments: str):
    return update_providers_cell(provider_id, 'comments', comments)

# ========================================================================== UPDATE CELL CLIENTS
@app.put("/update_client_name/")
def update_client_name(client_id: int, name: str):
    return update_clients_cell(client_id, 'name', name)

@app.put("/update_client_entity/")
def update_client_entity(client_id: int, entity: str):
    return update_clients_cell(client_id, 'entity', entity)

@app.put("/update_client_address/")
def update_client_address(client_id: int, address: str):
    return update_clients_cell(client_id, 'address', address)

@app.put("/update_client_address_comments/")
def update_client_address_comments(client_id: int, address_comments: str):
    return update_clients_cell(client_id, 'address_comments', address_comments)

@app.put("/update_client_network/")
def update_client_network(client_id: int, network: str):
    return update_clients_cell(client_id, 'network', network)

@app.put("/update_client_payment/")
def update_client_payment(client_id: int, payment: str):
    return update_clients_cell(client_id, 'payment', payment)

@app.put("/update_client_default_provider/")
def update_client_name(client_id: int, default_provider_id: int):
    return update_clients_cell(client_id, 'default_provider', default_provider_id)

@app.put("/update_client_recoil/")
def update_client_recoil(client_id: int, recoil: float):
    return update_clients_cell(client_id, 'recoil', recoil)

@app.put("/update_client_comments/")
def update_client_name(client_id: int, comments: str):
    return update_clients_cell(client_id, 'comments', comments)
# ========================================================================== UPDATE CELL CLIENTS_WORK_HOURS
@app.put("/update_client_wh_monday/")
def update_client_wh_monday(client_id: int, monday: str):
    return update_clients_work_hours_cell(client_id, 'monday', monday)

@app.put("/update_client_wh_tuesday/")
def update_client_wh_tuesday(client_id: int, tuesday: str):
    return update_clients_work_hours_cell(client_id, 'tuesday', tuesday)

@app.put("/update_client_wh_wednesday/")
def update_client_wh_wednesday(client_id: int, wednesday: str):
    return update_clients_work_hours_cell(client_id, 'wednesday', wednesday)

@app.put("/update_client_wh_thursday/")
def update_client_wh_thursday(client_id: int, thursday: str):
    return update_clients_work_hours_cell(client_id, 'thursday', thursday)

@app.put("/update_client_wh_friday/")
def update_client_wh_friday(client_id: int, friday: str):
    return update_clients_work_hours_cell(client_id, 'friday', friday)

@app.put("/update_client_wh_saturday/")
def update_client_wh_saturday(client_id: int, saturday: str):
    return update_clients_work_hours_cell(client_id, 'saturday', saturday)

@app.put("/update_client_wh_sunday/")
def update_client_wh_sunday(client_id: int, sunday: str):
    return update_clients_work_hours_cell(client_id, 'sunday', sunday)
# ========================================================================== UPDATE CELL PROVIDERS_PURCHASES

@app.put("/update_purchase_delivery_time/")
def update_purchase_delivery_time(purchase_id: int, delivery_time: str):
    return update_providers_purchases_cell(purchase_id, 'delivery_time', delivery_time)

@app.put("/update_purchase_provider/")
def update_purchase_provider(purchase_id: int, provider_id: int):
    return update_providers_purchases_cell(purchase_id, 'provider', provider_id)

@app.put("/update_purchase_product/")
def update_purchase_product(purchase_id: int, product: str):
    return update_providers_purchases_cell(purchase_id, 'product', product)

@app.put("/update_purchase_amount/")
def update_purchase_amount(purchase_id: int, amount: int):
    return update_providers_purchases_cell(purchase_id, 'amount', amount)

@app.put("/update_purchase_weight/")
def update_purchase_weight(purchase_id: int, weight: float):
    return update_providers_purchases_cell(purchase_id, 'weight', weight)

@app.put("/update_purchase_price_per_kilo/")
def update_purchase_price_per_kilo(purchase_id: int, price_per_kilo: float):
    return update_providers_purchases_cell(purchase_id, 'price_per_kilo', price_per_kilo)

@app.put("/update_purchase_total_price/")
def update_purchase_total_price(purchase_id: int, total_price: float):
    return update_providers_purchases_cell(purchase_id, 'total_price', total_price)

@app.put("/update_purchase_paid/")
def update_purchase_paid(purchase_id: int, paid: float):
    return update_providers_purchases_cell(purchase_id, 'paid', paid)

@app.put("/update_purchase_debt/")
def update_purchase_debt(purchase_id: int, debt: float):
    return update_providers_purchases_cell(purchase_id, 'debt', debt)

@app.put("/update_purchase_comments/")
def update_purchase_comments(purchase_id: int, comments: str):
    return update_providers_purchases_cell(purchase_id, 'comments', comments)

@app.put("/update_purchase_status/")
def update_purchase_status(purchase_id: int, status: str):
    return update_providers_purchases_cell(purchase_id, 'status', status)
# ========================================================================== UPDATE CELL CLIENTS_SALES

@app.put("/update_sale_delivery_time/")
def update_sale_delivery_time(sale_id: int, delivery_time: str):
    return update_clients_sales_cell(sale_id, 'delivery_time', delivery_time)

@app.put("/update_sale_client/")
def update_sale_client(sale_id: int, client_id: int):
    return update_clients_sales_cell(sale_id, 'client_id', client_id)

@app.put("/update_sale_provider/")
def update_sale_provider(sale_id: int, provider_id: int):
    return update_clients_sales_cell(sale_id, 'provider_id', provider_id)

@app.put("/update_sale_driver/")
def update_sale_driver(sale_id: int, driver_id: int):
    return update_clients_sales_cell(sale_id, 'driver_id', driver_id)

@app.put("/update_sale_paid/")
def update_sale_paid(sale_id: int, paid: float):
    return update_clients_sales_cell(sale_id, 'paid', paid)

@app.put("/update_sale_debt/")
def update_sale_debt(sale_id: int, debt: float):
    return update_clients_sales_cell(sale_id, 'debt', debt)

@app.put("/update_sale_comments/")
def update_sale_comments(sale_id: int, comments: str):
    return update_clients_sales_cell(sale_id, 'comments', comments)

@app.put("/update_sale_status/")
def update_sale_status(sale_id: int, status: str):
    return update_clients_sales_cell(sale_id, 'status', status)
# ========================================================================== UPDATE CELL DRIVERS_SHARE
@app.put("/update_share_driver/")
def update_share_driver(share_id: int, driver_id: int):
    return update_drivers_share_cell(share_id, 'driver_id', driver_id)

@app.put("/update_share_purchase/")
def update_share_purchase(share_id: int, purchase_id: int):
    return update_drivers_share_cell(share_id, 'purchase_id', purchase_id)

@app.put("/update_share_amount/")
def update_share_amount(share_id: int, amount: int):
    return update_drivers_share_cell(share_id, 'amount', amount)

@app.put("/update_share_weight/")
def update_share_weight(share_id: int, weight: float):
    return update_drivers_share_cell(share_id, 'weight', weight)

@app.put("/update_share_price_per_kilo/")
def update_share_price_per_kilo(share_id: int, price_per_kilo: int):
    return update_drivers_share_cell(share_id, 'price_per_kilo', price_per_kilo)

@app.put("/update_share_status/")
def update_share_status(share_id: int, status: str):
    return update_drivers_share_cell(share_id, 'status', status)

# ========================================================================== UPDATE CELL HISTORY
@app.put("/update_story_sale/")
def update_story_sale(story_id: int, sale_id: int):
    return update_history_cell(story_id, 'sale_id', sale_id)

@app.put("/update_story_share/")
def update_story_share(story_id: int, share_id: int):
    return update_history_cell(story_id, 'share_id', share_id)

@app.put("/update_story_amount/")
def update_story_amount(story_id: int, amount: int):
    return update_history_cell(story_id, 'amount', amount)

@app.put("/update_story_weight/")
def update_story_weight(story_id: int, weight: float):
    return update_history_cell(story_id, 'weight', weight)

@app.put("/update_story_price_per_kilo/")
def update_story_price_per_kilo(story_id: int, price_per_kilo: float):
    return update_history_cell(story_id, 'price_per_kilo', price_per_kilo)

@app.put("/update_story_total_price/")
def update_story_total_price(story_id: int, total_price: float):
    return update_history_cell(story_id, 'total_price', total_price)

# ========================================================================== UPDATE CELL CLIENTS FUTURE SALES
@app.put("/update_future_sale_client/")
def update_future_sale_client(f_sale_id: int, client_id: int):
    return update_clients_future_sales_cell(f_sale_id, 'client', client_id)

@app.put("/update_future_sale_product/")
def update_future_sale_product(f_sale_id: int, product: str):
    return update_clients_future_sales_cell(f_sale_id, 'product', product)

@app.put("/update_future_sale_amount/")
def update_future_sale_amount(f_sale_id: int, amount: int):
    return update_clients_future_sales_cell(f_sale_id, 'amount', amount)

@app.put("/update_future_sale_order_time/")
def update_future_sale_order_time(f_sale_id: int, order_time: str):
    return update_clients_future_sales_cell(f_sale_id, 'order_time', order_time)

@app.put("/update_future_sale_delivery_time/")
def update_future_sale_delivery_time(f_sale_id: int, delivery_time: str):
    return update_clients_future_sales_cell(f_sale_id, 'delivery_time', delivery_time)

@app.put("/update_future_sale_status/")
def update_future_sale_status(f_sale_id: int, status: str):
    return update_clients_future_sales_cell(f_sale_id, 'status', status)

@app.put("/update_future_sale_comments/")
def update_future_sale_comments(f_sale_id: int, comments: str):
    return update_clients_future_sales_cell(f_sale_id, 'comments', comments)

# ========================================================================== UPDATE CELL PRODUCTS
@app.put("/update_products_product_name/")
def update_products_product_name(product_id: int, product_name: str):
    return update_products_cell(product_id, 'product_name', product_name)

# ========================================================================== UPDATE CELL CLIENTS PRICES
@app.put("/update_clients_prices_product_name/")
def update_clients_prices_product_name(client_price_id: int, product_name: str):
    return update_clients_prices_cell(client_price_id, 'product_name', product_name)

@app.put("/update_clients_prices_client/")
def update_clients_prices_client(client_price_id: int, client_id: int):
    return update_clients_prices_cell(client_price_id, 'client_id', client_id)

@app.put("/update_clients_prices_price/")
def update_clients_prices_price(client_price_id: int, price: float):
    return update_clients_prices_cell(client_price_id, 'price', price)


# ========================================================================== CHECK USER PW AND ROLE
@app.get("/check_users_pw_and_role/")
def check_users_pw_and_role(login: str, password: str, role: str):
    conn = None
    try:
        params = config_database()
        
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        get_users_info_by_login_sql = "select users.id,\
                                              users.name,\
                                              users.contacts,\
                                              users.login,\
                                              users.password,\
                                              users_roles.is_admin,\
                                              users_roles.is_driver,\
                                              users_roles.is_operator,\
                                              users_roles.is_superuser from users\
                                              left join users_roles on\
                                              users.id = users_roles.user_id where users.login=%s limit 1;"

        cur.itersize = 200
                                                
        cur.execute(get_users_info_by_login_sql, (login,))

        is_exist = cur.fetchone()

        if not is_exist:
            return {
                "error": "user with this login doesn't exist"
            }

        is_correct_user = False

        if is_exist[4] == password:
            if role == "admin":
                if is_exist[5]:
                    is_correct_user = True
            elif role == "driver":
                if is_exist[6]:
                    is_correct_user = True
            elif role == "operator":
                if is_exist[7]:
                    is_correct_user = True
            elif role == "superuser":
                if is_exist[8]:
                    is_correct_user = True


        users_json = { "id": is_exist[0],
                       "name": is_exist[1],
                       "contacts": is_exist[2],
                       "login": is_exist[3],
                       "password": is_exist[4],
                       "roles:": {
                           "is_admin": is_exist[5],
                           "is_driver": is_exist[6],
                           "is_operator": is_exist[7],
                           "is_superuser": is_exist[8]
                       },
                       "is_correct_user": is_correct_user }

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT USER BY LOGIN successfully")
        
        return {
            "user": users_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()


@app.get("/get_warehouse/")
def get_warehouse():
    conn = None
    try:
        params = config_database()

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        get_warehouse_sql_2 = "select pp.id,\
                                      p.id,\
                                      p.name,\
                                      p.contacts,\
                                      p.comments,\
                                      pp.product,\
                                      coalesce((pp.amount - coalesce(share.amount, 0)), 0) as remainder_amount,\
                                      coalesce((pp.weight - coalesce(share.weight, 0)), 0) as remainder_weight,\
                                      pp.price_per_kilo,\
                                      pp.delivery_time from providers_purchases pp\
                                        left join providers p on pp.provider = p.id\
                                        left join (select ds.purchase_id, coalesce(sum(ds.amount), 0) as amount,\
                                                   coalesce(sum(ds.weight), 0) as weight from drivers_share ds group by purchase_id)\
                                        share on pp.id = share.purchase_id where (pp.amount - coalesce(share.amount, 0)) > 0;"

        cur.itersize = 200
                                                
        cur.execute(get_warehouse_sql_2)

        warehouse_json = {product[0]: { "provider": {
                                            "id": product[1],
                                            "name": product[2],
                                            "contacts": product[3],
                                            "comments": product[4]
                                        },
                                        "product": product[5],
                                        "amount": product[6],
                                        "weight": product[7],
                                        "price_per_kilo": product[8],
                                        "delivery_time": product[9] } for product in cur}

        cur.close()

        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT WAREHOUSE successfully")

        return {
            "warehouse": warehouse_json
        }

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception("Exception occurred")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()