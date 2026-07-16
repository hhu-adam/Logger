# Logger

Internal logger for Lean4Game.

It does two things:

1. Collects usage statistics from open Lean4Game sessions.
2. Checks which installed games are inactive and can eventually be cleaned up.

## Usage statistics

The Logger reads the Lean4Game sessions API and stores daily country/game statistics.

## Game activity cleanup

Lean4Game stores when a game was last seen, played, or uploaded. The Logger reads this data once per day and calculates the game status.

Stages:

```text
active -> inactive -> deletion_due -> moved to .trash -> deleted
```

(trash entries are deleted after `GAME_TRASH_RETENTION_DAYS`).


The Logger writes reports to:

```text
Activity/logs/activity-status.json
Activity/logs/cleanup-status.json
```

The Logger skips protected games, currently open games, invalid paths, symlinks, and missing folders.

## Prometheus
[TODO]
