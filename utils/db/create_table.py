# from sqlalchemy import *
#
# from utils.db.db_config import get_db_engine
#
# meta = MetaData()
# engine = get_db_engine()
#
# opera_occ_model = Table(
#     'opera_occ', meta,
#     Column('id', Integer, primary_key=True, autoincrement=True),
#     Column('propertyCode', String(20)),
#     Column('pullDateId', String(20)),
#     Column('REVENUE', String(55)),
#     Column('NO_ROOMS', String(55)),
#     Column('IND_DEDUCT_ROOMS', String(55)),
#     Column('IND_NON_DEDUCT_ROOMS', String(55)),
#     Column('GRP_DEDUCT_ROOMS', String(55)),
#     Column('GRP_NON_DEDUCT_ROOMS', String(55)),
#     Column('NO_PERSONS', String(55)),
#     Column('ARRIVAL_ROOMS', String(55)),
#     Column('DEPARTURE_ROOMS', String(55)),
#     Column('COMPLIMENTARY_ROOMS', String(55)),
#     Column('HOUSE_USE_ROOMS', String(55)),
#     Column('DAY_USE_ROOMS', String(55)),
#     Column('NO_SHOW_ROOMS', String(55)),
#     Column('INVENTORY_ROOMS', String(55)),
#     Column('CONSIDERED_DATE', String(55)),
#     Column('CHAR_CONSIDERED_DATE', String(55)),
#     Column('IND_DEDUCT_REVENUE', String(55)),
#     Column('IND_NON_DEDUCT_REVENUE', String(55)),
#     Column('GRP_NON_DEDUCT_REVENUE', String(55)),
#     Column('GRP_DEDUCT_REVENUE', String(55)),
#     Column('OWNER_ROOMS', String(55)),
#     Column('FF_ROOMS', String(55)),
#     Column('CF_OOO_ROOMS', String(55)),
#     Column('CF_CALC_OCC_ROOMS', String(55)),
#     Column('CF_CALC_INV_ROOMS', String(55)),
#     Column('CF_AVERAGE_ROOM_RATE', String(55)),
#     Column('CF_OCCUPANCY', String(55)),
#     Column('CF_IND_DED_REV', String(55)),
#     Column('CF_IND_NON_DED_REV', String(55)),
#     Column('CF_BLK_DED_REV', String(55)),
#     Column('CF_BLK_NON_DED_REV', String(55))
# )
#
# meta.create_all(engine)
