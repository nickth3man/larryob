from datetime import date

from pydantic import BaseModel, ConfigDict, Field, model_validator


class BaseGameLogRow(BaseModel):
    """Shared rules for Player and Team game logs."""

    model_config = ConfigDict(extra="ignore")

    fgm: int | None = Field(default=None, ge=0)
    fga: int | None = Field(default=None, ge=0)
    fg3m: int | None = Field(default=None, ge=0)
    fg3a: int | None = Field(default=None, ge=0)
    ftm: int | None = Field(default=None, ge=0)
    fta: int | None = Field(default=None, ge=0)
    oreb: int | None = Field(default=None, ge=0)
    dreb: int | None = Field(default=None, ge=0)
    reb: int | None = Field(default=None, ge=0)
    ast: int | None = Field(default=None, ge=0)
    stl: int | None = Field(default=None, ge=0)
    blk: int | None = Field(default=None, ge=0)
    tov: int | None = Field(default=None, ge=0)
    pf: int | None = Field(default=None, ge=0)
    pts: int | None = Field(default=None, ge=0)
    plus_minus: int | None = None

    @model_validator(mode="after")
    def validate_shooting_and_rebounds(self) -> "BaseGameLogRow":
        if self.fgm is not None and self.fga is not None and self.fgm > self.fga:
            raise ValueError("FGM cannot be greater than FGA")
        if self.fg3m is not None and self.fg3a is not None and self.fg3m > self.fg3a:
            raise ValueError("FG3M cannot be greater than FG3A")
        if self.ftm is not None and self.fta is not None and self.ftm > self.fta:
            raise ValueError("FTM cannot be greater than FTA")
        if (
            self.oreb is not None
            and self.dreb is not None
            and self.reb is not None
            and self.oreb + self.dreb != self.reb
        ):
            raise ValueError("OREB + DREB must equal REB")
        return self


class PlayerGameLogRow(BaseGameLogRow):
    game_id: str
    player_id: str
    team_id: str
    minutes_played: float | None = Field(default=None, ge=0)
    starter: int | None = None


class TeamGameLogRow(BaseGameLogRow):
    game_id: str
    team_id: str


class FactGameRow(BaseModel):
    model_config = ConfigDict(extra="ignore")
    game_id: str
    home_score: int | None = Field(default=None, ge=0)
    away_score: int | None = Field(default=None, ge=0)
    game_date: date | None = None


class FactSalaryRow(BaseModel):
    model_config = ConfigDict(extra="ignore")
    player_id: str
    team_id: str
    season_id: str
    salary: int | None = Field(default=None, ge=0)


class FactPlayerSeasonStatsRow(BaseModel):
    model_config = ConfigDict(extra="ignore")
    fg: int | None = Field(default=None, ge=0)
    fga: int | None = Field(default=None, ge=0)
    x3p: int | None = Field(default=None, ge=0)
    x3pa: int | None = Field(default=None, ge=0)
    ft: int | None = Field(default=None, ge=0)
    fta: int | None = Field(default=None, ge=0)
    pts: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_shooting(self) -> "FactPlayerSeasonStatsRow":
        if self.fg is not None and self.fga is not None and self.fg > self.fga:
            raise ValueError("fg > fga")
        if self.x3p is not None and self.x3pa is not None and self.x3p > self.x3pa:
            raise ValueError("x3p > x3pa")
        if self.ft is not None and self.fta is not None and self.ft > self.fta:
            raise ValueError("ft > fta")
        return self


class FactPlayerAdvancedSeasonRow(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ts_pct: float | None = Field(default=None, ge=0.0, le=1.5)
    orb_pct: float | None = Field(default=None, ge=0.0, le=1.0)
    drb_pct: float | None = Field(default=None, ge=0.0, le=1.0)
    usg_pct: float | None = Field(default=None, ge=0.0, le=1.0)


class FactPlayerShootingSeasonRow(BaseModel):
    model_config = ConfigDict(extra="ignore")
    pct_fga_0_3: float | None = None
    pct_fga_3_10: float | None = None
    pct_fga_10_16: float | None = None
    pct_fga_16_3p: float | None = None
    pct_fga_3p: float | None = None

    @model_validator(mode="after")
    def validate_zones(self) -> "FactPlayerShootingSeasonRow":
        zones = [
            self.pct_fga_0_3,
            self.pct_fga_3_10,
            self.pct_fga_10_16,
            self.pct_fga_16_3p,
            self.pct_fga_3p,
        ]
        if all(z is not None for z in zones):
            zone_sum = sum(zones)
            if abs(zone_sum - 1.0) > 0.05:
                raise ValueError(f"zone_sum is {zone_sum:.3f}, expected ~1.0")
        return self
