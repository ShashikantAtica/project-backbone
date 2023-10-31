# from sqlalchemy import *
#
# from utils.db.db_config import get_db_engine
#
# meta = MetaData()
# engine = get_db_engine()
#
# bestrev_total_forecast_model = Table(
#     'bestrev_total_forecast', meta,
#     Column('id', Integer, primary_key=True, autoincrement=True),
#     Column('propertyCode', String(55)),
#     Column('pullDateId', BIGINT),
#     Column('Alert', String(55)),
#     Column('Priority', String(55)),
#     Column('StayDate', String(55)),
#     Column('DayofWeek', String(55)),
#     Column('Favorite', String(55)),
#     Column('Event', String(55)),
#     Column('BestWesternRate', String(55)),
#     Column('RecommendedRate', String(55)),
#     Column('RatetoUpload', String(55)),
#     Column('RecommendationStatus', String(55)),
#     Column('MarketRate', String(55)),
#     Column('AvailableRooms', String(55)),
#     Column('TransientCapacity', String(55)),
#     Column('TotalForecast_IncludesGroup', String(55)),
#     Column('OntheBooks_IncludesGroup', String(55)),
#     Column('AverageDailyRate', String(55)),
#     Column('RevPAR', String(55)),
#     Column('Occupancy_IncludesGroup', String(55)),
#     Column('ForecastOccupancy_IncludesGroup', String(55)),
# )
#
# meta.create_all(engine)
