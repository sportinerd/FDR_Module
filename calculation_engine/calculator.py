# engine/calculator.py
import logging
import numpy as np
from sqlalchemy.orm import Session
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta

from data.models import (
    Fixture, Team, League, HistoricalMatch, 
    Odds, SeasonOutright, PlayerAvailability
)
from config.leagues import LEAGUE_CONFIGS, DEFAULT_MAJOR_LEAGUE_CONFIG, DEFAULT_SMALLER_LEAGUE_CONFIG
from engine.fallback import (
    estimate_historical_score, estimate_form_score,
    estimate_odds_score, estimate_outright_score
)

logger = logging.getLogger(__name__)

class FDRCalculator:
    def __init__(self, db: Session):
        self.db = db
    
    def calculate_fdr_for_fixture(self, fixture_id: int) -> bool:
        """
        Calculate FDR for a specific fixture
        Returns True if successful, False otherwise
        """
        try:
            # Get fixture
            fixture = self.db.query(Fixture).filter(Fixture.id == fixture_id).first()
            if not fixture:
                logger.error(f"Fixture with ID {fixture_id} not found")
                return False
            
            # Get teams
            home_team = self.db.query(Team).filter(Team.id == fixture.home_team_id).first()
            away_team = self.db.query(Team).filter(Team.id == fixture.away_team_id).first()
            
            # Get league
            league = self.db.query(League).filter(League.id == fixture.league_id).first()
            
            # Get league configuration
            league_config = LEAGUE_CONFIGS.get(
                league.code, 
                DEFAULT_MAJOR_LEAGUE_CONFIG if league.is_major else DEFAULT_SMALLER_LEAGUE_CONFIG
            )
            
            # Calculate FDR components
            
            # 1. Historical Data Score
            historical_score_home = self.calculate_historical_score(
                home_team.id, away_team.id, league.id, is_home=True, is_derby=fixture.is_derby
            )
            historical_score_away = self.calculate_historical_score(
                away_team.id, home_team.id, league.id, is_home=False, is_derby=fixture.is_derby
            )
            
            # 2. Recent Form Score
            form_score_home = self.calculate_form_score(home_team.id)
            form_score_away = self.calculate_form_score(away_team.id)
            
            # 3. Season Outright Score (if applicable)
            outright_score_home = 0
            outright_score_away = 0
            if league.is_major:
                outright_score_home = self.calculate_outright_score(home_team.id)
                outright_score_away = self.calculate_outright_score(away_team.id)
            
            # 4. Fixture Odds Score
            odds_score_home, odds_score_away = self.calculate_odds_score(fixture.id)
            
            # 5. Player Availability Score
            player_score_home = self.calculate_player_availability_score(
                home_team.id, fixture.gameweek, datetime.now().year
            )
            player_score_away = self.calculate_player_availability_score(
                away_team.id, fixture.gameweek, datetime.now().year
            )
            
            # Apply weights based on league configuration
            weights = league_config['weights']
            
            # Calculate overall FDR for home team
            home_overall_fdr = (
                weights['historical'] * historical_score_home +
                weights['form'] * form_score_home +
                (weights.get('outright', 0) * outright_score_home if league.is_major else 0) +
                weights['odds'] * odds_score_home +
                weights['player_availability'] * player_score_home
            )
            
            # Calculate overall FDR for away team
            away_overall_fdr = (
                weights['historical'] * historical_score_away +
                weights['form'] * form_score_away +
                (weights.get('outright', 0) * outright_score_away if league.is_major else 0) +
                weights['odds'] * odds_score_away +
                weights['player_availability'] * player_score_away
            )
            
            # Calculate specialized metrics (attacking, defending, clean sheet)
            home_attacking_fdr = self.calculate_attacking_fdr(
                home_overall_fdr, historical_score_home, odds_score_home
            )
            away_attacking_fdr = self.calculate_attacking_fdr(
                away_overall_fdr, historical_score_away, odds_score_away
            )
            
            home_defending_fdr = self.calculate_defending_fdr(
                home_overall_fdr, historical_score_home, odds_score_home
            )
            away_defending_fdr = self.calculate_defending_fdr(
                away_overall_fdr, historical_score_away, odds_score_away
            )
            
            home_clean_sheet_fdr = self.calculate_clean_sheet_fdr(
                home_overall_fdr, historical_score_home, odds_score_home
            )
            away_clean_sheet_fdr = self.calculate_clean_sheet_fdr(
                away_overall_fdr, historical_score_away, odds_score_away
            )
            
            # Scale FDR values to 0-10 range
            home_overall_fdr_scaled = self.scale_to_fdr_range(home_overall_fdr)
            away_overall_fdr_scaled = self.scale_to_fdr_range(away_overall_fdr)
            
            home_attacking_fdr_scaled = self.scale_to_fdr_range(home_attacking_fdr)
            away_attacking_fdr_scaled = self.scale_to_fdr_range(away_attacking_fdr)
            
            home_defending_fdr_scaled = self.scale_to_fdr_range(home_defending_fdr)
            away_defending_fdr_scaled = self.scale_to_fdr_range(away_defending_fdr)
            
            home_clean_sheet_fdr_scaled = self.scale_to_fdr_range(home_clean_sheet_fdr)
            away_clean_sheet_fdr_scaled = self.scale_to_fdr_range(away_clean_sheet_fdr)
            
            # Update fixture with FDR values
            fixture.home_overall_fdr = home_overall_fdr_scaled
            fixture.away_overall_fdr = away_overall_fdr_scaled
            
            fixture.home_attacking_fdr = home_attacking_fdr_scaled
            fixture.away_attacking_fdr = away_attacking_fdr_scaled
            
            fixture.home_defending_fdr = home_defending_fdr_scaled
            fixture.away_defending_fdr = away_defending_fdr_scaled
            
            fixture.home_clean_sheet_fdr = home_clean_sheet_fdr_scaled
            fixture.away_clean_sheet_fdr = away_clean_sheet_fdr_scaled
            
            self.db.commit()
            
            logger.info(f"FDR calculated for fixture {fixture_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error calculating FDR for fixture {fixture_id}: {str(e)}")
            self.db.rollback()
            return False
    
    def calculate_historical_score(
        self, team_id: int, opponent_id: int, league_id: int, 
        is_home: bool, is_derby: bool
    ) -> float:
        """
        Calculate historical performance score between two teams
        Returns a score between 0-1 where higher indicates tougher fixture
        """
        try:
            # Get historical matches between these teams
            historical_matches = self.db.query(HistoricalMatch).filter(
                ((HistoricalMatch.home_team_id == team_id) & 
                 (HistoricalMatch.away_team_id == opponent_id)) | 
                ((HistoricalMatch.home_team_id == opponent_id) & 
                 (HistoricalMatch.away_team_id == team_id)),
                HistoricalMatch.league_id == league_id
            ).order_by(HistoricalMatch.match_date.desc()).all()
            
            if not historical_matches:
                # No historical data, use fallback
                return estimate_historical_score(team_id, opponent_id, self.db)
            
            # Calculate win rate
            total_matches = 0
            team_wins = 0
            draws = 0
            
            # Apply recency weighting - more recent matches have higher weight
            current_year = datetime.now().year
            weights = []
            
            for match in historical_matches:
                match_year = match.match_date.year
                # Weight decreases by 10% per year
                weight = max(0.1, 1 - 0.1 * (current_year - match_year))
                weights.append(weight)
                
                if match.home_team_id == team_id:
                    if match.home_score > match.away_score:
                        team_wins += weight
                    elif match.home_score == match.away_score:
                        draws += weight
                else:  # Away team
                    if match.away_score > match.home_score:
                        team_wins += weight
                    elif match.home_score == match.away_score:
                        draws += weight
                
                total_matches += weight
            
            # Calculate weighted win rate
            if total_matches > 0:
                win_rate = team_wins / total_matches
                draw_rate = draws / total_matches
                
                # Convert win rate to difficulty score (1 - win_rate)
                # Higher win rate means easier fixture (lower difficulty)
                difficulty = 1 - win_rate - (0.5 * draw_rate)
                
                # Apply derby boost if applicable
                if is_derby:
                    # Get league configuration
                    league = self.db.query(League).filter(League.id == league_id).first()
                    league_config = LEAGUE_CONFIGS.get(
                        league.code, 
                        DEFAULT_MAJOR_LEAGUE_CONFIG if league.is_major else DEFAULT_SMALLER_LEAGUE_CONFIG
                    )
                    
                    # Apply derby boost - derbies are typically harder
                    derby_boost = league_config.get('derby_difficulty_boost', 0.15)
                    difficulty = min(1.0, difficulty + derby_boost)
                
                return difficulty
            
            # Fallback if no weighted matches
            return 0.5
            
        except Exception as e:
            logger.error(f"Error calculating historical score: {str(e)}")
            return 0.5  # Default to medium difficulty
    
    def calculate_form_score(self, team_id: int) -> float:
        """
        Calculate recent form score based on last 3 matches
        Returns a score between 0-1 where higher indicates tougher fixture
        """
        try:
            # Get last 3 matches for this team
            recent_matches = self.db.query(Fixture).filter(
                (Fixture.home_team_id == team_id) | (Fixture.away_team_id == team_id),
                Fixture.match_date < datetime.now(),
                Fixture.home_score.isnot(None),  # Match is completed
                Fixture.away_score.isnot(None)
            ).order_by(Fixture.match_date.desc()).limit(3).all()
            
            if not recent_matches or len(recent_matches) < 3:
                # Not enough recent matches, use fallback
                return estimate_form_score(team_id, self.db)
            
            # Calculate form score
            form_points = 0
            max_points = 0
            
            for i, match in enumerate(recent_matches):
                # More recent matches have higher weight
                weight = 1.0 - (i * 0.2)  # 1.0, 0.8, 0.6
                max_points += weight * 3  # Maximum possible points
                
                if match.home_team_id == team_id:
                    if match.home_score > match.away_score:
                        form_points += weight * 3  # Win
                    elif match.home_score == match.away_score:
                        form_points += weight * 1  # Draw
                else:  # Away team
                    if match.away_score > match.home_score:
                        form_points += weight * 3  # Win
                    elif match.home_score == match.away_score:
                        form_points += weight * 1  # Draw
            
            # Calculate form ratio
            if max_points > 0:
                form_ratio = form_points / max_points
                
                # Convert form ratio to difficulty score (1 - form_ratio)
                # Higher form ratio means easier fixture (lower difficulty)
                return 1 - form_ratio
            
            # Fallback
            return 0.5
            
        except Exception as e:
            logger.error(f"Error calculating form score: {str(e)}")
            return 0.5  # Default to medium difficulty
    
    def calculate_outright_score(self, team_id: int) -> float:
        """
        Calculate difficulty based on season outright odds
        Returns a score between 0-1 where higher indicates tougher fixture
        """
        try:
            # Get current season
            current_season = str(datetime.now().year)
            
            # Get season outright odds for this team
            outright = self.db.query(SeasonOutright).filter(
                SeasonOutright.team_id == team_id,
                SeasonOutright.season == current_season
            ).order_by(SeasonOutright.timestamp.desc()).first()
            
            if not outright or not outright.championship_odds:
                # No outright odds, use fallback
                return estimate_outright_score(team_id, self.db)
            
            # Convert odds to implied probability
            # Lower odds = higher probability = stronger team
            implied_probability = 1 / outright.championship_odds
            
            # Normalize probability to 0-1 range
            # This is a simplified normalization - in practice, you would
            # compare against all teams in the league
            normalized_probability = min(implied_probability, 0.5) / 0.5
            
            # Convert to difficulty score (1 - normalized_probability)
            # Higher probability means easier fixture (lower difficulty)
            return 1 - normalized_probability
            
        except Exception as e:
            logger.error(f"Error calculating outright score: {str(e)}")
            return 0.5  # Default to medium difficulty
    
    def calculate_odds_score(self, fixture_id: int) -> Tuple[float, float]:
        """
        Calculate difficulty based on match odds
        Returns two scores (home, away) between 0-1 where higher indicates tougher fixture
        """
        try:
            # Get latest odds for this fixture
            odds = self.db.query(Odds).filter(
                Odds.fixture_id == fixture_id
            ).order_by(Odds.timestamp.desc()).first()
            
            if not odds:
                # No odds data, use fallback
                fixture = self.db.query(Fixture).filter(Fixture.id == fixture_id).first()
                return estimate_odds_score(fixture.home_team_id, fixture.away_team_id, self.db)
            
            # Convert odds to implied probabilities
            p_home = 1 / odds.home_win if odds.home_win > 0 else 0
            p_draw = 1 / odds.draw if odds.draw > 0 else 0
            p_away = 1 / odds.away_win if odds.away_win > 0 else 0
            
            # Normalize probabilities to sum to 1
            total = p_home + p_draw + p_away
            if total > 0:
                p_home /= total
                p_draw /= total
                p_away /= total
            
            # Calculate difficulty for home team (from perspective of home team)
            # Higher probability of winning means easier fixture (lower difficulty)
            home_difficulty = 1 - p_home - (0.5 * p_draw)
            
            # Calculate difficulty for away team (from perspective of away team)
            # Higher probability of winning means easier fixture (lower difficulty)
            away_difficulty = 1 - p_away - (0.5 * p_draw)
            
            return home_difficulty, away_difficulty
            
        except Exception as e:
            logger.error(f"Error calculating odds score: {str(e)}")
            return 0.5, 0.5  # Default to medium difficulty
    
    def calculate_player_availability_score(
        self, team_id: int, gameweek: int, season: int
    ) -> float:
        """
        Calculate difficulty based on player availability
        Returns a score between 0-1 where higher indicates tougher fixture
        """
        try:
            # Get player availability for this team and gameweek
            players = self.db.query(PlayerAvailability).filter(
                PlayerAvailability.team_id == team_id,
                PlayerAvailability.gameweek == gameweek,
                PlayerAvailability.season == str(season)
            ).all()
            
            if not players:
                # No player data, assume all key players available
                return 0.0  # Easiest difficulty
            
            # Calculate availability score
            total_importance = 0
            missing_importance = 0
            
            for player in players:
                total_importance += player.importance
                
                if player.status != "Available":
                    missing_importance += player.importance
            
            # Calculate ratio of missing importance
            if total_importance > 0:
                missing_ratio = missing_importance / total_importance
                
                # Convert to difficulty score
                # Higher missing ratio means harder fixture (higher difficulty)
                return missing_ratio
            
            # Fallback
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating player availability score: {str(e)}")
            return 0.0  # Default to easiest difficulty
    
    def calculate_attacking_fdr(
        self, overall_fdr: float, historical_score: float, odds_score: float
    ) -> float:
        """
        Calculate attacking-specific FDR
        This would be customized based on attacking metrics
        """
        # Simplified implementation - would be more sophisticated in practice
        # For attacking, we weight recent form and odds more heavily
        return 0.7 * overall_fdr + 0.3 * (1 - odds_score)
    
    def calculate_defending_fdr(
        self, overall_fdr: float, historical_score: float, odds_score: float
    ) -> float:
        """
        Calculate defending-specific FDR
        This would be customized based on defensive metrics
        """
        # Simplified implementation - would be more sophisticated in practice
        # For defending, we weight historical performance more heavily
        return 0.6 * overall_fdr + 0.4 * historical_score
    
    def calculate_clean_sheet_fdr(
        self, overall_fdr: float, historical_score: float, odds_score: float
    ) -> float:
        """
        Calculate clean sheet potential FDR
        This would be customized based on clean sheet metrics
        """
        # Simplified implementation - would be more sophisticated in practice
        # For clean sheets, we weight historical defensive performance heavily
        return 0.5 * overall_fdr + 0.5 * historical_score
    
    def scale_to_fdr_range(self, raw_score: float) -> float:
        """
        Scale raw difficulty score (0-1) to FDR range (0-10)
        """
        # Ensure raw_score is within 0-1
        raw_score = max(0, min(1, raw_score))
        
        # Scale to 0-10 range
        return raw_score * 10
    
    def get_fdr_category(self, fdr_score: float) -> str:
        """
        Get FDR category based on the score
        """
        if 0 <= fdr_score <= 2:
            return "EASIEST"
        elif 3 <= fdr_score <= 4:
            return "EASIER"
        elif 5 <= fdr_score <= 6:
            return "AVERAGE"
        elif 7 <= fdr_score <= 8:
            return "TOUGH"
        else:  # 9-10
            return "TOUGHEST"
    
    def get_fdr_color(self, fdr_score: float) -> str:
        """
        Get color code based on FDR score
        """
        category = self.get_fdr_category(fdr_score)
        return {
            "EASIEST": "#00E563",
            "EASIER": "#80FFB7",
            "AVERAGE": "#E1EBE5",
            "TOUGH": "#FFA0A0",
            "TOUGHEST": "#FF6060"
        }.get(category, "#E1EBE5")  # Default to AVERAGE color
