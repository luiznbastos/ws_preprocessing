import json
import os
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Union
from enum import Enum
from pydantic import BaseModel


class playerIdNameDictionary:
    pass


class referee(BaseModel):
    officialId: int
    firstName: str
    lastName: str
    hasParticipatedMatches: bool
    name: str


class periodMinuteLimits:
    pass


class team(BaseModel):
    teamId: int
    formations: list[Any]
    stats: Dict[str, Any]
    incidentEvents: list[Any]
    shotZones: Dict[str, Any]
    name: str
    countryName: str
    players: List[Any]
    managerName: str
    scores: Dict[str, int]
    field: str
    averageAge: float


class home(team):
    pass


class away(team):
    pass


class event(BaseModel):
    id: float
    eventId: int
    minute: int
    second: Optional[int] = None
    teamId: int
    playerId: Optional[int] = None
    x: float
    y: float
    expandedMinute: int
    period: Dict[str, Any]
    type: Dict[str, Any]
    outcomeType: Dict[str, Any]
    qualifiers: List[Dict[str, Union[Dict[str, Any], Any]]]
    satisfiedEventsTypes: List[int]
    isTouch: bool
    blockedX: Optional[float] = None
    blockedY: Optional[float] = None
    endX: Optional[float] = None
    endY: Optional[float] = None
    goalMouthZ: Optional[float] = None
    goalMouthY: Optional[float] = None
    isShot: bool
    relatedEventId: Optional[float] = None
    relatedPlayerId: Optional[float] = None
    cardType: Optional[Dict[str, Any]] = None
    isGoal: Optional[bool] = None
    isOwnGoal: Optional[bool] = None

# resultados = [
#     {"value": evento["type"]["value"], "displayName": evento["type"]["displayName"]}
#     for evento in data["matchCentreData"]["events"]
#     if evento["type"]["value"] == 2
# ]


class formation(BaseModel):
    formationId: int
    formationName: str
    captainPlayerId: int
    period: int
    startMinuteExpanded: int
    endMinuteExpanded: int
    jerseyNumber: List[int]
    formationSlots: List[int]
    playerIds: List[int]
    formationPosition: List[Dict[str, float]]


class matchCentreData(BaseModel):
    playerIdNameDictionary: Dict[str, str]
    periodMinuteLimits: Dict[str, int]
    timeStamp: str
    attendance: int
    venueName: str
    referee: referee
    weatherCode: str
    elapsed: str
    startTime: str
    startDate: str
    score: str
    htScore: str
    ftScore: str
    etScore: str
    pkScore: str
    statusCode: int
    periodCode: int
    home: home
    away: away
    maxMinute: int
    minuteExpanded: int
    maxPeriod: int
    expandedMinutes: Dict[str, Dict[str, int]]
    expandedMaxMinute: int
    periodEndMinutes: Dict[str, int]
    commonEvents: list[Any]
    events: List[event]
    timeoutInSeconds: int


class matchCentreEventTypeJson(BaseModel):
    shotSixYardBox: int
    shotPenaltyArea: int
    shotOboxTotal: int
    shotOpenPlay: int
    shotCounter: int
    shotSetPiece: int
    shotDirectCorner: int
    shotOffTarget: int
    shotOnPost: int
    shotOnTarget: int
    shotsTotal: int
    shotBlocked: int
    shotRightFoot: int
    shotLeftFoot: int
    shotHead: int
    shotObp: int
    goalSixYardBox: int
    goalPenaltyArea: int
    goalObox: int
    goalOpenPlay: int
    goalCounter: int
    goalSetPiece: int
    penaltyScored: int
    goalOwn: int
    goalNormal: int
    goalRightFoot: int
    goalLeftFoot: int
    goalHead: int
    goalObp: int
    shortPassInaccurate: int
    shortPassAccurate: int
    passCorner: int
    passCornerAccurate: int
    passCornerInaccurate: int
    passFreekick: int
    passBack: int
    passForward: int
    passLeft: int
    passRight: int
    keyPassLong: int
    keyPassShort: int
    keyPassCross: int
    keyPassCorner: int
    keyPassThroughball: int
    keyPassFreekick: int
    keyPassThrowin: int
    keyPassOther: int
    assistCross: int
    assistCorner: int
    assistThroughball: int
    assistFreekick: int
    assistThrowin: int
    assistOther: int
    dribbleLost: int
    dribbleWon: int
    challengeLost: int
    interceptionWon: int
    clearanceHead: int
    outfielderBlock: int
    passCrossBlockedDefensive: int
    outfielderBlockedPass: int
    offsideGiven: int
    offsideProvoked: int
    foulGiven: int
    foulCommitted: int
    yellowCard: int
    voidYellowCard: int
    secondYellow: int
    redCard: int
    turnover: int
    dispossessed: int
    saveLowLeft: int
    saveHighLeft: int
    saveLowCentre: int
    saveHighCentre: int
    saveLowRight: int
    saveHighRight: int
    saveHands: int
    saveFeet: int
    saveObp: int
    saveSixYardBox: int
    savePenaltyArea: int
    saveObox: int
    keeperDivingSave: int
    standingSave: int
    closeMissHigh: int
    closeMissHighLeft: int
    closeMissHighRight: int
    closeMissLeft: int
    closeMissRight: int
    shotOffTargetInsideBox: int
    touches: int
    assist: int
    ballRecovery: int
    clearanceEffective: int
    clearanceTotal: int
    clearanceOffTheLine: int
    dribbleLastman: int
    errorLeadsToGoal: int
    errorLeadsToShot: int
    intentionalAssist: int
    interceptionAll: int
    interceptionIntheBox: int
    keeperClaimHighLost: int
    keeperClaimHighWon: int
    keeperClaimLost: int
    keeperClaimWon: int
    keeperOneToOneWon: int
    parriedDanger: int
    parriedSafe: int
    collected: int
    keeperPenaltySaved: int
    keeperSaveInTheBox: int
    keeperSaveTotal: int
    keeperSmother: int
    keeperSweeperLost: int
    keeperMissed: int
    passAccurate: int
    passBackZoneInaccurate: int
    passForwardZoneAccurate: int
    passInaccurate: int
    passAccuracy: int
    cornerAwarded: int
    passKey: int
    passChipped: int
    passCrossAccurate: int
    passCrossInaccurate: int
    passLongBallAccurate: int
    passLongBallInaccurate: int
    passThroughBallAccurate: int
    passThroughBallInaccurate: int
    passThroughBallInacurate: int
    passFreekickAccurate: int
    passFreekickInaccurate: int
    penaltyConceded: int
    penaltyMissed: int
    penaltyWon: int
    passRightFoot: int
    passLeftFoot: int
    passHead: int
    sixYardBlock: int
    tackleLastMan: int
    tackleLost: int
    tackleWon: int
    cleanSheetGK: int
    cleanSheetDL: int
    cleanSheetDC: int
    cleanSheetDR: int
    cleanSheetDML: int
    cleanSheetDMC: int
    cleanSheetDMR: int
    cleanSheetML: int
    cleanSheetMC: int
    cleanSheetMR: int
    cleanSheetAML: int
    cleanSheetAMC: int
    cleanSheetAMR: int
    cleanSheetFWL: int
    cleanSheetFW: int
    cleanSheetFWR: int
    cleanSheetSub: int
    goalConcededByTeamGK: int
    goalConcededByTeamDL: int
    goalConcededByTeamDC: int
    goalConcededByTeamDR: int
    goalConcededByTeamDML: int
    goalConcededByTeamDMC: int
    goalConcededByTeamDMR: int
    goalConcededByTeamML: int
    goalConcededByTeamMC: int
    goalConcededByTeamMR: int
    goalConcededByTeamAML: int
    goalConcededByTeamAMC: int
    goalConcededByTeamAMR: int
    goalConcededByTeamFWL: int
    goalConcededByTeamFW: int
    goalConcededByTeamFWR: int
    goalConcededByTeamSub: int
    goalConcededOutsideBoxGoalkeeper: int
    goalScoredByTeamGK: int
    goalScoredByTeamDL: int
    goalScoredByTeamDC: int
    goalScoredByTeamDR: int
    goalScoredByTeamDML: int
    goalScoredByTeamDMC: int
    goalScoredByTeamDMR: int
    goalScoredByTeamML: int
    goalScoredByTeamMC: int
    goalScoredByTeamMR: int
    goalScoredByTeamAML: int
    goalScoredByTeamAMC: int
    goalScoredByTeamAMR: int
    goalScoredByTeamFWL: int
    goalScoredByTeamFW: int
    goalScoredByTeamFWR: int
    goalScoredByTeamSub: int
    aerialSuccess: int
    duelAerialWon: int
    duelAerialLost: int
    offensiveDuel: int
    defensiveDuel: int
    bigChanceMissed: int
    bigChanceScored: int
    bigChanceCreated: int
    overrun: int
    successfulFinalThirdPasses: int
    punches: int
    penaltyShootoutScored: int
    penaltyShootoutMissedOffTarget: int
    penaltyShootoutSaved: int
    penaltyShootoutSavedGK: int
    penaltyShootoutConcededGK: int
    throwIn: int
    subOn: int
    subOff: int
    defensiveThird: int
    midThird: int
    finalThird: int
    pos: int


# TODO: Criar enums para os tipos de eventos
# TODO: Criar enums para os tipos de outcometype
# TODO: Criar enums para os tipos de period
# TODO: Criar enums para os tipos de qualifiers
# TODO: Criar maneira de transformar os dicts referentes aos enum acima em enums antes de validar o schema com o pydantic


# if __name__ == "__main__":

#     centre_data = matchCentreData(**data["matchCentreData"])
#     event_types = matchCentreEventTypeJson(**data["matchCentreEventTypeJson"])
#     arbitro = referee(**data["matchCentreData"]["referee"])
#     time = team(**data["matchCentreData"]["home"])
#     fora = team(**data["matchCentreData"]["away"])
#     evento = event(**data["matchCentreData"]["events"][0])


Stop = True