"""
설정 관리 클래스
환경 변수와 YAML 설정 파일을 통합 관리
"""

import os
import yaml
from typing import Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DatabaseConfig:
    """데이터베이스 설정"""
    host: str
    port: int
    name: str
    user: str
    password: str
    
    @property
    def connection_string(self) -> str:
        """PostgreSQL 연결 문자열 생성"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


@dataclass
class RedisConfig:
    """Redis 설정"""
    host: str
    port: int
    db: int
    password: Optional[str] = None


@dataclass
class AWSConfig:
    """AWS 설정"""
    region: str
    s3_bucket: str


@dataclass
class ModelConfig:
    """모델 설정"""
    version: str
    target_column: str
    feature_window_days: int
    prediction_window_days: int
    thresholds: Dict[str, float]
    ensemble_weights: Dict[str, float]


@dataclass
class PipelineConfig:
    """파이프라인 설정"""
    batch_size: int
    feature_store_refresh_hours: int
    model_retrain_days: int


@dataclass
class APIConfig:
    """API 설정"""
    host: str
    port: int
    workers: int
    timeout: int


class Settings:
    """
    애플리케이션 설정 관리 클래스
    YAML 파일과 환경 변수를 통합하여 설정을 관리
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        설정 초기화
        
        Args:
            config_path: 설정 파일 경로
        """
        self.config_path = Path(config_path)
        self._config_data = self._load_config()
        
        # 각 설정 섹션 초기화
        self.database = self._init_database_config()
        self.redis = self._init_redis_config()
        self.aws = self._init_aws_config()
        self.model = self._init_model_config()
        self.pipeline = self._init_pipeline_config()
        self.api = self._init_api_config()
        self.features = self._config_data.get("features", {})
        self.logging = self._config_data.get("logging", {})
    
    def _load_config(self) -> Dict[str, Any]:
        """
        YAML 설정 파일 로드 및 환경 변수 치환
        
        Returns:
            설정 딕셔너리
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config_content = f.read()
        
        # 환경 변수 치환
        config_content = self._substitute_env_vars(config_content)
        
        return yaml.safe_load(config_content)
    
    def _substitute_env_vars(self, content: str) -> str:
        """
        설정 파일 내 환경 변수 치환
        ${VAR_NAME:default_value} 형식 지원
        
        Args:
            content: 원본 설정 파일 내용
            
        Returns:
            환경 변수가 치환된 설정 파일 내용
        """
        import re
        
        def replace_env_var(match):
            var_expr = match.group(1)
            if ':' in var_expr:
                var_name, default_value = var_expr.split(':', 1)
                return os.getenv(var_name, default_value)
            else:
                return os.getenv(var_expr, '')
        
        # ${VAR_NAME:default} 또는 ${VAR_NAME} 패턴 치환
        pattern = r'\$\{([^}]+)\}'
        return re.sub(pattern, replace_env_var, content)
    
    def _init_database_config(self) -> DatabaseConfig:
        """데이터베이스 설정 초기화"""
        db_config = self._config_data.get("database", {})
        return DatabaseConfig(
            host=db_config.get("host", "localhost"),
            port=int(db_config.get("port", 5432)),
            name=db_config.get("name", "subscription_hub"),
            user=db_config.get("user", "postgres"),
            password=db_config.get("password", "password")
        )
    
    def _init_redis_config(self) -> RedisConfig:
        """Redis 설정 초기화"""
        redis_config = self._config_data.get("redis", {})
        return RedisConfig(
            host=redis_config.get("host", "localhost"),
            port=int(redis_config.get("port", 6379)),
            db=int(redis_config.get("db", 0)),
            password=redis_config.get("password") or None
        )
    
    def _init_aws_config(self) -> AWSConfig:
        """AWS 설정 초기화"""
        aws_config = self._config_data.get("aws", {})
        return AWSConfig(
            region=aws_config.get("region", "ap-northeast-2"),
            s3_bucket=aws_config.get("s3_bucket", "churn-prediction-data")
        )
    
    def _init_model_config(self) -> ModelConfig:
        """모델 설정 초기화"""
        model_config = self._config_data.get("model", {})
        return ModelConfig(
            version=model_config.get("version", "v1.0.0"),
            target_column=model_config.get("target_column", "churn_label"),
            feature_window_days=int(model_config.get("feature_window_days", 30)),
            prediction_window_days=int(model_config.get("prediction_window_days", 14)),
            thresholds=model_config.get("thresholds", {
                "high_risk": 0.7,
                "medium_risk": 0.4,
                "low_risk": 0.2
            }),
            ensemble_weights=model_config.get("ensemble_weights", {
                "gbdt": 0.6,
                "lstm": 0.4
            })
        )
    
    def _init_pipeline_config(self) -> PipelineConfig:
        """파이프라인 설정 초기화"""
        pipeline_config = self._config_data.get("pipeline", {})
        return PipelineConfig(
            batch_size=int(pipeline_config.get("batch_size", 1000)),
            feature_store_refresh_hours=int(pipeline_config.get("feature_store_refresh_hours", 6)),
            model_retrain_days=int(pipeline_config.get("model_retrain_days", 30))
        )
    
    def _init_api_config(self) -> APIConfig:
        """API 설정 초기화"""
        api_config = self._config_data.get("api", {})
        return APIConfig(
            host=api_config.get("host", "0.0.0.0"),
            port=int(api_config.get("port", 8000)),
            workers=int(api_config.get("workers", 4)),
            timeout=int(api_config.get("timeout", 30))
        )
    
    def get_feature_columns(self) -> list:
        """
        모든 피처 컬럼 목록 반환
        
        Returns:
            피처 컬럼 리스트
        """
        all_features = []
        for category, features in self.features.items():
            if isinstance(features, list):
                all_features.extend(features)
        return all_features
    
    def get_risk_level(self, score: float) -> str:
        """
        점수를 기반으로 위험도 레벨 결정
        
        Args:
            score: 예측 점수 (0-1)
            
        Returns:
            위험도 레벨 ('low', 'medium', 'high')
        """
        if score >= self.model.thresholds["high_risk"]:
            return "high"
        elif score >= self.model.thresholds["medium_risk"]:
            return "medium"
        else:
            return "low"
    
    def reload_config(self):
        """설정 파일 다시 로드"""
        self._config_data = self._load_config()
        self.__init__(str(self.config_path))


# 전역 설정 인스턴스
settings = Settings()
