"""Backfill loader registry configuration."""

from dataclasses import dataclass


@dataclass
class LoaderConfig:
    """Configuration for a single backfill loader."""

    name: str
    table_name: str
    loader_name: str  # Function name for runtime lookup


LOADERS: list[LoaderConfig] = [
    LoaderConfig("team_history", "dim_team_history", "load_team_history"),
    LoaderConfig("dim_team_enrich", "dim_team", "enrich_dim_team"),
    LoaderConfig("dim_player_enrich", "dim_player", "enrich_dim_player"),
    LoaderConfig("player_career", "dim_player", "enrich_player_career"),
    LoaderConfig("games", "fact_game", "load_games"),
    LoaderConfig("schedule", "fact_game", "load_schedule"),
    LoaderConfig("player_game_logs", "player_game_log", "load_player_game_logs"),
    LoaderConfig("team_game_logs", "team_game_log", "load_team_game_logs"),
    LoaderConfig("team_season", "fact_team_season", "load_team_season"),
    LoaderConfig("league_season", "dim_league_season", "load_league_season"),
    LoaderConfig("draft", "fact_draft", "load_draft"),
    LoaderConfig("player_season_stats", "fact_player_season_stats", "load_player_season_stats"),
    LoaderConfig("player_advanced", "fact_player_advanced_season", "load_player_advanced"),
    LoaderConfig("player_shooting", "fact_player_shooting_season", "load_player_shooting"),
    LoaderConfig("player_pbp_season", "fact_player_pbp_season", "load_player_pbp_season"),
    LoaderConfig("bulk_pbp", "fact_play_by_play", "load_bulk_pbp"),
    LoaderConfig("salary_history", "fact_salary", "_load_salary_history_adapter"),
    LoaderConfig("awards", "fact_player_award", "load_awards"),
    LoaderConfig("all_star", "fact_all_star", "load_all_star_selections"),
    LoaderConfig("all_nba", "fact_all_nba", "load_all_nba_teams"),
    LoaderConfig("all_nba_votes", "fact_all_nba_vote", "load_all_nba_votes"),
]
