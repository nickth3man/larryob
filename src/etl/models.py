
from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator


def validate_common_game_log_rules(obj: Any) -> Any:
    if getattr(obj, "fgm", None) is not None and getattr(obj, "fga", None) is not None:
        if obj.fgm > obj.fga:
            raise ValueError("fgm > fga")
    if getattr(obj, "fg3m", None) is not None and getattr(obj, "fg3a", None) is not None:
        if obj.fg3m > obj.fg3a:
            raise ValueError("fg3m > fg3a")
    if getattr(obj, "ftm", None) is not None and getattr(obj, "fta", None) is not None:
        if obj.ftm > obj.fta:
            raise ValueError("ftm > fta")
    if getattr(obj, "pts", None) is not None and obj.pts < 0:
        raise ValueError("pts < 0")
    if getattr(obj, "oreb", None) is not None and getattr(obj, "dreb", None) is not None and getattr(obj, "reb", None) is not None:
        if obj.oreb + obj.dreb != obj.reb:
            raise ValueError("oreb + dreb != reb")
    return obj


class PlayerGameLogRow(BaseModel):
    model_config = ConfigDict(extra='ignore')

    game_id: str
    player_id: str
    team_id: str
    minutes_played: float | None = None
    fgm: int | None = None
    fga: int | None = None
    fg3m: int | None = None
    fg3a: int | None = None
    ftm: int | None = None
    fta: int | None = None
    oreb: int | None = None
    dreb: int | None = None
    reb: int | None = None
    ast: int | None = None
    stl: int | None = None
    blk: int | None = None
    tov: int | None = None
    pf: int | None = None
    pts: int | None = None
    plus_minus: int | None = None
    starter: int | None = None

    @model_validator(mode="after")
    def validate_rules(self) -> 'PlayerGameLogRow':
        validate_common_game_log_rules(self)
        # minutes >= 0
        if self.minutes_played is not None and self.minutes_played < 0:
            raise ValueError("minutes_played < 0")

        return self


class TeamGameLogRow(BaseModel):
    model_config = ConfigDict(extra='ignore')

    game_id: str
    team_id: str
    fgm: int | None = None
    fga: int | None = None
    fg3m: int | None = None
    fg3a: int | None = None
    ftm: int | None = None
    fta: int | None = None
    oreb: int | None = None
    dreb: int | None = None
    reb: int | None = None
    ast: int | None = None
    stl: int | None = None
    blk: int | None = None
    tov: int | None = None
    pf: int | None = None
    pts: int | None = None
    plus_minus: int | None = None

    @model_validator(mode="after")
    def validate_rules(self) -> 'TeamGameLogRow':
        validate_common_game_log_rules(self)
        return self
