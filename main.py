import logging
from time import gmtime, localtime, strftime
from typing import Dict, List, Optional, Any

import psycopg2
from psycopg2.sql import SQL, Identifier
from config_db import config_database
from fastapi import FastAPI

app = FastAPI() # uvicorn main:app --host 195.2.93.21 --port 80

root_logger= logging.getLogger()
root_logger.setLevel(logging.INFO)
handler = logging.FileHandler('cheese_api.log', 'w', 'utf-8')
formatter = logging.Formatter('%(levelname)s - %(message)s')
handler.setFormatter(formatter)
root_logger.addHandler(handler)


@app.get("/hello/")
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
                                                       comments) values (%s, %s, %s);"
                                                
        cur.execute(create_provider_sql, (name,
                                          contacts, 
                                          comments))
        
        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW PROVIDER | {name} | created successfully")

        return {
            "new_provider": {
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
def create_new_purchase(order_time: str, 
                        delivery_time: str, 
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

        create_purchase_sql = "insert into providers_purchases ( order_time,\
                                                                 delivery_time,\
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
                                                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                                                
        cur.execute(create_purchase_sql, ( order_time, # 1999-01-08 04:05:06 timestamp input format
                                           delivery_time, # 1999-01-08 04:05:06
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

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW PURCHASE | prod: {product} | prov: {provider_name} created successfully")

        return {
            "new_purhcase": {
                "order_time": order_time,
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
def create_new_sale(order_time: str, 
                    delivery_time: str,
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

        create_sale_sql = "insert into clients_sales ( order_time,\
                                                       delivery_time,\
                                                       client,\
                                                       provider,\
                                                       driver,\
                                                       paid,\
                                                       debt,\
                                                       comments,\
                                                       status )\
                                                values (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
                                                
        cur.execute(create_sale_sql, ( order_time,
                                       delivery_time,
                                       client_id,
                                       provider_id,
                                       driver_id,
                                       paid,
                                       debt,
                                       comments,
                                       status ))

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW SALE | c: {client_name} | p: {provider_name} | d: {driver_name} created successfully")

        return {
            "new_sale": {
                "order_time": order_time,
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
                                                        price_per_kilo\
                                                        )\
                                                values (%s, %s, %s, %s, %s);"
                                                
        cur.execute(create_share_sql, ( driver_id,
                                        purchase_id,
                                        amount,
                                        weight,
                                        price_per_kilo ))

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW SHARE | d: {driver_id} | pur: {purchase_id} created successfully")

        return {
            "new_share": {
                "driver_id": driver_id,
                "purchase_id": purchase_id,
                "amount": amount,
                "weight": weight,
                "price_per_kilo": price_per_kilo
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
                                                  total_price ) values (%s, %s, %s, %s, %s, %s);"
                                                
        cur.execute(create_story_sql, ( sale_id,
                                        share_id,
                                        amount,
                                        weight,
                                        price_per_kilo,
                                        total_price ))

        cur.close()
        
        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---NEW STORY | s: {sale_id} | sh: {share_id} created successfully")

        return {
            "new_story": {
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


# ========================================================================== GET
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
def get_all_purchases():
    conn = None
    try:
        params = config_database()

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        get_all_purchases_sql = "select providers_purchases.id,\
                                      order_time,\
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
                                        left join providers on providers_purchases.provider = providers.id;"

        cur.itersize = 200
                                                
        cur.execute(get_all_purchases_sql)

        purchases_json = {purchase[0]: { "purchase_id": purchase[1],
                                         "order_time": purchase[2],
                                         "delivery_time": purchase[3],
                                         "provider": {
                                             "name": purchase[4],
                                             "contacts": purchase[5],
                                             "comments": purchase[6]
                                         },
                                         "product": purchase[7],
                                         "amount": purchase[8],
                                         "weight": purchase[9],
                                         "price_per_kilo": purchase[10],
                                         "total_price": purchase[11],
                                         "paid": purchase[12],
                                         "debt": purchase[13],
                                         "comments": purchase[14],
                                         "status": purchase[15] } for purchase in cur}

        cur.close()

        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL PURCHASES successfully")

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
def get_all_sales():
    conn = None
    try:
        params = config_database()

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        get_all_sales_sql = "select cs.id,\
                                    cs.order_time,\
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
                                    left join providers def_prov on s_client.default_provider = def_prov.id;"

        cur.itersize = 200
                                                
        cur.execute(get_all_sales_sql)

        sales_json = {sale[0]: { 
            "order_time": sale[1],
            "delivery_time": sale[2],
            "client": {
                "id": sale[3],
                "name": sale[4],
                "entity": sale[5],
                "address": sale[6],
                "address_comments": sale[7],
                "network": sale[8],
                "payment": sale[9],
                "default_provider_id": sale[10],
                "default_provider": {
                    "id": sale[11],
                    "name": sale[12],
                    "contacts": sale[13],
                    "comments": sale[14]
                },
                "recoil": sale[15],
                "comments": sale[16],
                "work_hours": {
                    "monday": sale[17],
                    "tuesday": sale[18],
                    "wednesday": sale[19],
                    "thursday": sale[20],
                    "friday": sale[21],
                    "saturday": sale[22],
                    "sunday": sale[23]
                }
            },
            "provider": {
                "id": sale[24],
                "name": sale[25],
                "contacts": sale[26],
                "comments": sale[27]
            },
            "driver": {
                "id": sale[28],
                "name": sale[29],
                "contacts": sale[30]
            },
            "paid": sale[31],
            "debt": sale[32],
            "comments": sale[33],
            "status": sale[34]
        } for sale in cur}

        cur.close()

        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL SALES successfully")

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
def get_all_shares():
    conn = None
    try:
        params = config_database()

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        get_all_share_sql = "select ds.id,\
                                    dr.id,\
                                    dr.name,\
                                    dr.contacts,\
                                    pp.id,\
                                    pp.order_time,\
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
                                    ds.price_per_kilo from drivers_share ds\
                                    left join users dr on ds.driver_id = dr.id\
                                    left join providers_purchases pp on ds.purchase_id = pp.id\
                                    left join providers pr on pp.provider = pr.id;"

        cur.itersize = 200
                                                
        cur.execute(get_all_share_sql)

        shares_json = {share[0]: { 
            "driver": {
                "id": share[1],
                "name": share[2],
                "contacts": share[3]
            },
            "purchase": {
                "id": share[4],
                "order_time": share[5],
                "delivery_time": share[6],
                "provider": {
                    "id": share[7],
                    "name": share[8],
                    "contacts": share[9],
                    "comments": share[10]
                },
                "product": share[11],
                "amount": share[12],
                "weight": share[13],
                "price_per_kilo": share[14],
                "total_price": share[15],
                "paid": share[16],
                "debt": share[17],
                "comments": share[18],
                "status": share[19]
            },
            "amount": share[20],
            "weight": share[21],
            "price_per_kilo": share[22]
        } for share in cur}

        cur.close()

        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL SHARES successfully")

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
def get_all_history():
    conn = None
    try:
        params = config_database()

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        get_all_history_sql = "select h.id,\
                                    cs.id,\
                                    cs.order_time,\
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
                                    pp.order_time,\
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
                                                
        cur.execute(get_all_history_sql)

        history_json = {story[0]: { 
            "sale": {
                "id": story[1],
                "order_time": story[2],
                "delivery_time": story[3],
                "client": {
                    "id": story[4],
                    "name": story[5],
                    "entity": story[6],
                    "address": story[7],
                    "address_comments": story[8],
                    "network": story[9],
                    "payment": story[10],
                    "default_provider_id": story[11],
                    "default_provider": {
                        "id": story[12],
                        "name": story[13],
                        "contacts": story[14],
                        "comments": story[15]
                    },
                    "recoil": story[16],
                    "comments": story[17],
                    "work_hours": {
                        "monday": story[18],
                        "tuesday": story[19],
                        "wednesday": story[20],
                        "thursday": story[21],
                        "friday": story[22],
                        "saturday": story[23],
                        "sunday": story[24]
                    }
                },
                "provider": {
                    "id": story[25],
                    "name": story[26],
                    "contacts": story[27],
                    "comments": story[28]
                },
                "driver": {
                    "id": story[29],
                    "name": story[30],
                    "contacts": story[31]
                },
                "paid": story[32],
                "debt": story[33],
                "comments": story[34],
                "status": story[35]
            },
            "share": {
                "id": story[36], 
                "driver": {
                    "id": story[37],
                    "name": story[38],
                    "contacts": story[39]
                },
                "purchase": {
                    "id": story[40],
                    "order_time": story[41],
                    "delivery_time": story[42],
                    "provider": {
                        "id": story[43],
                        "name": story[44],
                        "contacts": story[45],
                        "comments": story[46]
                    },
                    "product": story[47],
                    "amount": story[48],
                    "weight": story[49],
                    "price_per_kilo": story[50],
                    "total_price": story[51],
                    "paid": story[52],
                    "debt": story[53],
                    "comments": story[54],
                    "status": story[55]
                },
                "amount": story[56],
                "weight": story[57],
                "price_per_kilo": story[58]
            },
            "amount": story[59],
            "weight": story[60],
            "price_per_kilo": story[61],
            "total_price": story[62]
        } for story in cur}

        cur.close()

        conn.commit()

        current_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        logging.info(f"{current_time}---GOT ALL HISTORY successfully")

        return {
            "history": history_json
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
                    "order_time": updated_pp_tuple[1],
                    "delivery_time": updated_pp_tuple[2],
                    "provider": updated_pp_tuple[3],
                    "product": updated_pp_tuple[4],
                    "amount": updated_pp_tuple[5],
                    "weight": updated_pp_tuple[6],
                    "price_per_kilo": updated_pp_tuple[7],
                    "total_price": updated_pp_tuple[8],
                    "paid": updated_pp_tuple[9],
                    "debt": updated_pp_tuple[10],
                    "comments": updated_pp_tuple[11],
                    "status": updated_pp_tuple[12]
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
                    "order_time": updated_sale_tuple[1],
                    "delivery_time": updated_sale_tuple[2],
                    "client": updated_sale_tuple[3],
                    "provider": updated_sale_tuple[4],
                    "driver": updated_sale_tuple[5],
                    "paid": updated_sale_tuple[6],
                    "debt": updated_sale_tuple[7],
                    "comments": updated_sale_tuple[8],
                    "status": updated_sale_tuple[9]
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
                    "price_per_kilo": updated_share_tuple[5]
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
@app.put("/update_purchase_order_time/")
def update_purchase_order_time(purchase_id: int, order_time: str):
    return update_providers_purchases_cell(purchase_id, 'order_time', order_time)

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
@app.put("/update_sale_order_time/")
def update_sale_order_time(sale_id: int, order_time: str):
    return update_clients_sales_cell(sale_id, 'order_time', order_time)

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