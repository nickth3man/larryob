from typing import Optional
try:
    from pydantic import BaseModel, ConfigDict, Field, model_validator
except ImportError:
    # Fallback for environments where pydantic isn't installed
    BaseModel = object  # type: ignore
    model_validator = lambda *args, **kwargs: lambda f: f
    Field = lambda *args, **kwargs: None
    ConfigDict = dict

class PlayerGameLogRow(BaseModel):
    model_config = ConfigDict(extra='ignore')

    game_id: str
    player_id: str
    team_id: str
    minutes_played: Optional[float] = None
    fgm: Optional[int] = None
    fga: Optional[int] = None
    fg3m: Optional[int] = None
    fg3a: Optional[int] = None
    ftm: Optional[int] = None
    fta: Optional[int] = None
    oreb: Optional[int] = None
    dreb: Optional[int] = None
    reb: Optional[int] = None
    ast: Optional[int] = None
    stl: Optional[int] = None
    blk: Optional[int] = None
    tov: Optional[int] = None
    pf: Optional[int] = None
    pts: Optional[int] = None
    plus_minus: Optional[int] = None
    starter: Optional[int] = None

    @model_validator(mode="after")
    def validate_rules(self) -> 'PlayerGameLogRow':
        # fgm <= fga
        if self.fgm is not None and self.fga is not None:
            if self.fgm > self.fga:
                raise ValueError("fgm > fga")
        # fg3m <= fg3a
        if self.fg3m is not None and self.fg3a is not None:
            if self.fg3m > self.fg3a:
                raise ValueError("fg3m > fg3a")
        # ftm <= fta
        if self.ftm is not None and self.fta is not None:
            if self.ftm > self.fta:
                raise ValueError("ftm > fta")
        # pts >= 0
        if self.pts is not None and self.pts < 0:
            raise ValueError("pts < 0")
        # oreb + dreb == reb
        if self.oreb is not None and self.dreb is not None and self.reb is not None:
            if self.oreb + self.dreb != self.reb:
                raise ValueError("oreb + dreb != reb")
        # minutes >= 0
        if self.minutes_played is not None and self.minutes_played < 0:
            raise ValueError("minutes_played < 0")

        return self


class TeamGameLogRow(BaseModel):
    model_config = ConfigDict(extra='ignore')

    game_id: str
    team_id: str
    fgm: Optional[int] = None
    fga: Optional[int] = None
    fg3m: Optional[int] = None
    fg3a: Optional[int] = None
    ftm: Optional[int] = None
    fta: Optional[int] = None
    oreb: Optional[int] = None
    dreb: Optional[int] = None
    reb: Optional[int] = None
    ast: Optional[int] = None
    stl: Optional[int] = None
    blk: Optional[int] = None
    tov: Optional[int] = None
    pf: Optional[int] = None
    pts: Optional[int] = None
    plus_minus: Optional[int] = None

    @model_validator(mode="after")
    def validate_rules(self) -> 'TeamGameLogRow':
        # fgm <= fga
        if self.fgm is not None and self.fga is not None:
            if self.fgm > self.fga:
                raise ValueError("fgm > fga")
        # fg3m <= fg3a
        if self.fg3m is not None and self.fg3a is not None:
            if self.fg3m > self.fg3a:
                raise ValueError("fg3m > fg3a")
        # ftm <= fta
        if self.ftm is not None and self.fta is not None:
            if self.ftm > self.fta:
                raise ValueError("ftm > fta")
        # pts >= 0
        if self.pts is not None and self.pts < 0:
            raise ValueError("pts < 0")
        # oreb + dreb == reb
        if self.oreb is not None and self.dreb is not None and self.reb is not None:
            if self.oreb + self.dreb != self.reb:
                raise ValueError("oreb + dreb != reb")

        return self
