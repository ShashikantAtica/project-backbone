# from sqlalchemy import *
#
# from utils.db.db_config import get_db_engine
#
# meta = MetaData()
# engine = get_db_engine()
#
# marriott_reservation_model = Table(
#     'marriott_reservation', meta,
#     Column('id', Integer, primary_key=True, autoincrement=True),
#     Column('propertyCode', String(55)),
#     Column('pullDateId', BIGINT),
#     Column('Year', String(55)),
#     Column('Month', String(55)),
#     Column('WeekdayWeekend', String(55)),
#     Column('DayOfWeek', String(55)),
#     Column('StayDate', String(55)),
#     Column('MarketCategory', String(55)),
#     Column('MarketSegment', String(55)),
#     Column('MarketPrefixMiniHotel', String(55)),
#     Column('RoomNights', String(55)),
#     Column('ADR', String(55)),
#     Column('Revenue', String(55))
# )
#
# meta.create_all(engine)
