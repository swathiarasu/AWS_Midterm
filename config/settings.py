from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    project_name: str = "backup-system"
    max_copies: int = 3
    cleaner_threshold_seconds: int = 10
    cleaner_schedule_minutes: int = 1