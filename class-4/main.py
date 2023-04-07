from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from models import Base
#from database import CONFIG
from dotenv import dotenv_values, load_dotenv

load_dotenv()

CONFIG = dotenv_values("class-4/.env")


def CreateEngine():
    engine = create_engine("mysql+mysqlconnector://%s:%s@%s:%s/%s" %
                           (CONFIG['MYSQL_USER'],
                            CONFIG['MYSQL_PASSWORD'], CONFIG['HOST'],CONFIG['PORT'],CONFIG['MYSQL_DATABASE'])
                           )


    connection = engine.connect()
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    return session