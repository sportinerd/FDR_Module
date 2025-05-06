# data/models.py
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import datetime

Base = declarative_base()

class League(Base):
    __tablename__ = 'leagues'
    
    id = Column(Integer, primary_key=True)
    sportmonks_id = Column(Integer, unique=True)
    name = Column(String, nullable=False)
    code = Column(String, nullable=False, unique=True)
    country = Column(String)
    is_major = Column(Boolean, default=False)  # Will be determined dynamically
    teams = relationship("Team", back_populates="league")
    
class Team(Base):
    __tablename__ = 'teams'
    
    id = Column(Integer, primary_key=True)
    sportmonks_id = Column(Integer, unique=True)
    name = Column(String, nullable=False)
    code = Column(String, nullable=False)
    league_id = Column(Integer, ForeignKey('leagues.id'))
    league = relationship("League", back_populates="teams")
    home_fixtures = relationship("Fixture", foreign_keys="Fixture.home_team_id")
    away_fixtures = relationship("Fixture", foreign_keys="Fixture.away_team_id")
    
class Fixture(Base):
    __tablename__ = 'fixtures'
    
    id = Column(Integer, primary_key=True)
    home_team_id = Column(Integer, ForeignKey('teams.id'))
    away_team_id = Column(Integer, ForeignKey('teams.id'))
    league_id = Column(Integer, ForeignKey('leagues.id'))
    match_date = Column(DateTime)
    gameweek = Column(Integer)
    season = Column(String)
    is_derby = Column(Boolean, default=False)
    
    # FDR scores for home team
    home_overall_fdr = Column(Float)
    home_attacking_fdr = Column(Float)
    home_defending_fdr = Column(Float)
    home_clean_sheet_fdr = Column(Float)
    
    # FDR scores for away team
    away_overall_fdr = Column(Float)
    away_attacking_fdr = Column(Float)
    away_defending_fdr = Column(Float)
    away_clean_sheet_fdr = Column(Float)
    
    # Results (to be filled after match)
    home_score = Column(Integer)
    away_score = Column(Integer)
    
class HistoricalMatch(Base):
    __tablename__ = 'historical_matches'
    
    id = Column(Integer, primary_key=True)
    home_team_id = Column(Integer, ForeignKey('teams.id'))
    away_team_id = Column(Integer, ForeignKey('teams.id'))
    league_id = Column(Integer, ForeignKey('leagues.id'))
    match_date = Column(DateTime)
    season = Column(String)
    home_score = Column(Integer)
    away_score = Column(Integer)
    is_derby = Column(Boolean, default=False)
    
class Odds(Base):
    __tablename__ = 'odds'
    
    id = Column(Integer, primary_key=True)
    fixture_id = Column(Integer, ForeignKey('fixtures.id'))
    provider = Column(String)  # e.g., "Bet365"
    home_win = Column(Float)
    draw = Column(Float)
    away_win = Column(Float)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    
class SeasonOutright(Base):
    __tablename__ = 'season_outrights'
    
    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey('teams.id'))
    league_id = Column(Integer, ForeignKey('leagues.id'))
    season = Column(String)
    championship_odds = Column(Float)
    top_4_odds = Column(Float)
    relegation_odds = Column(Float)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

class PlayerAvailability(Base):
    __tablename__ = 'player_availability'
    
    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey('teams.id'))
    player_name = Column(String)
    status = Column(String)  # "Available", "Injured", "Suspended"
    importance = Column(Float)  # 0-1 scale of player importance
    gameweek = Column(Integer)
    season = Column(String)
