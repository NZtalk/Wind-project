"""Data models."""
from . import db


class Windfarm(db.Model):
    __tablename__ = 'windfarm'
    
    id = db.Column(db.String(length=100), primary_key=True)
    code = db.Column(db.String(length=100))
    latitude = db.Column(db.Float, nullable=True)
    longitude = db.Column(db.Float, nullable=True)
    last_meteo_update = db.Column(db.DateTime, nullable=True)

    def __init__(self, id:str, code:str, latitude:float, longitude:float, last_meteo_update:datetime):
        self.id = id
        self.code = code
        self.latitude = latitude
        self.longitude = longitude
        self.last_meteo_update = last_meteo_update

    def __repr__(self):
        return '<Windfarm %r>' % self.code
    

class Windturbine(db.Model):
    __tablename__ = 'windfarm'
    
    id = db.Column(db.String(length=100), primary_key=True)
    code = db.Column(db.String(length=100))
    windfarm_id = db.Column(db.Integer, db.ForeignKey('windfarm.id'), nullable=False)
    latitude = db.Column(db.Float, nullable=True)
    longitude = db.Column(db.Float, nullable=True)
    last_scada_udpdate = db.Column(db.DateTime, nullable=True)

    def __init__(self, id:str, code:str, windfarm_id:str, latitude:float, longitude:float, last_meteo_update:datetime):
        self.id = id
        self.code = code
        self.windfarm_id = windfarm_id
        self.latitude = latitude
        self.longitude = longitude
        self.last_scada_udpdate = last_scada_udpdate

    def __repr__(self):
        return '<Windturbine %r>' % self.code