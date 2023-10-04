# from sqlalchemy import *
#
# from utils.db.db_config import get_db_engine
#
# meta = MetaData()
# engine = get_db_engine()
#
# marriott_realized_activity_model = Table(
#     'marriott_realized_activity', meta,
#     Column('id', Integer, primary_key=True, autoincrement=True),
#     Column('propertyCode', String(55)),
#     Column('pullDateId', BIGINT),
#     Column('ArrivalDate', String(55)),
#     Column('DOW', String(55)),
#     Column('EV_LO', String(55)),
#     Column('TransSold', String(55)),
#     Column('GroupSold', String(55)),
#     Column('TotalRoomsSold_Num', String(55)),
#     Column('TotalRoomsSold_Occ_Per', String(55)),
#     Column('ArvlAddlDem', String(55)),
#     Column('NoShows_Tran', String(55)),
#     Column('NoShows_Grp', String(55)),
#     Column('Cancels_TranRMS', String(55)),
#     Column('Cancels_TranLOS', String(55)),
#     Column('Cancels_TranTotal', String(55)),
#     Column('Cancels_Grp', String(55)),
#     Column('SameDayCheck_ins', String(55)),
#     Column('UnexpectedStaythroughs_Tran', String(55)),
#     Column('UnexpectedStaythroughs_Grp', String(55)),
#     Column('EarlyCheck_outs_Tran', String(55)),
#     Column('EarlyCheck_outs_Grp', String(55)),
# )
#
# marriott_total_yield_model = Table(
#     'marriott_total_yield', meta,
#     Column('id', Integer, primary_key=True, autoincrement=True),
#     Column('propertyCode', String(55)),
#     Column('pullDateId', BIGINT),
#     Column('Date', String(55)),
#     Column('Sleeping_Room_Occ_Per', String(55)),
#     Column('Function_Space_Occ_Per', String(55)),
#     Column('Sleeping_Rooms_Projected', String(55)),
#     Column('Transient', String(55)),
#     Column('inContract', String(55)),
#     Column('Definite', String(55)),
#     Column('Tentative_1', String(55)),
#     Column('Tentative_2', String(55)),
#     Column('Hold', String(55)),
#     Column('Prospect', String(55)),
#     Column('To_Be_s', String(55)),
#     Column('Out_of_Order_Rooms', String(55)),
#     Column('MARSHA_Booked_Group', String(55)),
#     Column('MARSHA_Blocked_Group', String(55)),
#     Column('Group_Restrictions_Hotel', String(55)),
#     Column('Restriction_Threshold_Hotel', String(55)),
# )
#
# meta.create_all(engine)
