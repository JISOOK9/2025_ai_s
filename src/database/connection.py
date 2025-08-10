"""
데이터베이스 연결 관리 클래스
PostgreSQL과 Redis 연결을 관리하고 세션을 제공
"""

import redis
from contextlib import contextmanager
from typing import Generator, Optional, Dict, Any, List
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from loguru import logger

from src.config.settings import settings


class DatabaseManager:
    """
    PostgreSQL 데이터베이스 연결 관리 클래스
    연결 풀링과 세션 관리를 담당
    """
    
    def __init__(self):
        """데이터베이스 매니저 초기화"""
        self._engine = None
        self._session_factory = None
        self._initialize_engine()
    
    def _initialize_engine(self):
        """SQLAlchemy 엔진 초기화"""
        try:
            # 연결 풀 설정으로 성능 최적화
            self._engine = create_engine(
                settings.database.connection_string,
                poolclass=QueuePool,
                pool_size=10,  # 기본 연결 풀 크기
                max_overflow=20,  # 최대 추가 연결 수
                pool_pre_ping=True,  # 연결 상태 확인
                pool_recycle=3600,  # 1시간마다 연결 재생성
                echo=False  # SQL 로깅 비활성화 (운영환경)
            )
            
            # 세션 팩토리 생성
            self._session_factory = sessionmaker(
                bind=self._engine,
                autocommit=False,
                autoflush=False
            )
            
            logger.info("데이터베이스 엔진이 성공적으로 초기화되었습니다.")
            
        except Exception as e:
            logger.error(f"데이터베이스 엔진 초기화 실패: {e}")
            raise
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        데이터베이스 세션 컨텍스트 매니저
        자동으로 트랜잭션을 관리하고 세션을 정리
        
        Yields:
            SQLAlchemy 세션 객체
        """
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"데이터베이스 트랜잭션 롤백: {e}")
            raise
        finally:
            session.close()
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        SQL 쿼리 실행 및 결과 반환
        
        Args:
            query: 실행할 SQL 쿼리
            params: 쿼리 파라미터
            
        Returns:
            쿼리 결과 리스트
        """
        try:
            with self.get_session() as session:
                result = session.execute(text(query), params or {})
                
                # SELECT 쿼리인 경우 결과 반환
                if query.strip().upper().startswith('SELECT'):
                    columns = result.keys()
                    rows = result.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
                else:
                    # INSERT, UPDATE, DELETE 등의 경우 영향받은 행 수 반환
                    return [{"affected_rows": result.rowcount}]
                    
        except Exception as e:
            logger.error(f"쿼리 실행 실패: {query[:100]}... - {e}")
            raise
    
    def execute_file(self, file_path: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        SQL 파일 실행
        
        Args:
            file_path: SQL 파일 경로
            params: 쿼리 파라미터
            
        Returns:
            쿼리 결과 리스트
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                query = f.read()
            
            return self.execute_query(query, params)
            
        except FileNotFoundError:
            logger.error(f"SQL 파일을 찾을 수 없습니다: {file_path}")
            raise
        except Exception as e:
            logger.error(f"SQL 파일 실행 실패: {file_path} - {e}")
            raise
    
    def check_connection(self) -> bool:
        """
        데이터베이스 연결 상태 확인
        
        Returns:
            연결 상태 (True: 정상, False: 비정상)
        """
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"데이터베이스 연결 확인 실패: {e}")
            return False
    
    def close(self):
        """데이터베이스 연결 종료"""
        if self._engine:
            self._engine.dispose()
            logger.info("데이터베이스 연결이 종료되었습니다.")


class RedisManager:
    """
    Redis 연결 관리 클래스
    캐시 및 세션 스토리지로 사용
    """
    
    def __init__(self):
        """Redis 매니저 초기화"""
        self._client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Redis 클라이언트 초기화"""
        try:
            self._client = redis.Redis(
                host=settings.redis.host,
                port=settings.redis.port,
                db=settings.redis.db,
                password=settings.redis.password,
                decode_responses=True,  # 문자열 자동 디코딩
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
            
            # 연결 테스트
            self._client.ping()
            logger.info("Redis 클라이언트가 성공적으로 초기화되었습니다.")
            
        except Exception as e:
            logger.error(f"Redis 클라이언트 초기화 실패: {e}")
            raise
    
    def get(self, key: str) -> Optional[str]:
        """
        Redis에서 값 조회
        
        Args:
            key: 조회할 키
            
        Returns:
            저장된 값 또는 None
        """
        try:
            return self._client.get(key)
        except Exception as e:
            logger.error(f"Redis GET 실패 - key: {key}, error: {e}")
            return None
    
    def set(self, key: str, value: str, expire: Optional[int] = None) -> bool:
        """
        Redis에 값 저장
        
        Args:
            key: 저장할 키
            value: 저장할 값
            expire: 만료 시간 (초)
            
        Returns:
            저장 성공 여부
        """
        try:
            return self._client.set(key, value, ex=expire)
        except Exception as e:
            logger.error(f"Redis SET 실패 - key: {key}, error: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """
        Redis에서 키 삭제
        
        Args:
            key: 삭제할 키
            
        Returns:
            삭제 성공 여부
        """
        try:
            return bool(self._client.delete(key))
        except Exception as e:
            logger.error(f"Redis DELETE 실패 - key: {key}, error: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """
        키 존재 여부 확인
        
        Args:
            key: 확인할 키
            
        Returns:
            키 존재 여부
        """
        try:
            return bool(self._client.exists(key))
        except Exception as e:
            logger.error(f"Redis EXISTS 실패 - key: {key}, error: {e}")
            return False
    
    def hget(self, name: str, key: str) -> Optional[str]:
        """
        해시에서 값 조회
        
        Args:
            name: 해시 이름
            key: 해시 키
            
        Returns:
            저장된 값 또는 None
        """
        try:
            return self._client.hget(name, key)
        except Exception as e:
            logger.error(f"Redis HGET 실패 - name: {name}, key: {key}, error: {e}")
            return None
    
    def hset(self, name: str, key: str, value: str) -> bool:
        """
        해시에 값 저장
        
        Args:
            name: 해시 이름
            key: 해시 키
            value: 저장할 값
            
        Returns:
            저장 성공 여부
        """
        try:
            return bool(self._client.hset(name, key, value))
        except Exception as e:
            logger.error(f"Redis HSET 실패 - name: {name}, key: {key}, error: {e}")
            return False
    
    def hgetall(self, name: str) -> Dict[str, str]:
        """
        해시의 모든 키-값 조회
        
        Args:
            name: 해시 이름
            
        Returns:
            해시의 모든 키-값 딕셔너리
        """
        try:
            return self._client.hgetall(name)
        except Exception as e:
            logger.error(f"Redis HGETALL 실패 - name: {name}, error: {e}")
            return {}
    
    def expire(self, key: str, time: int) -> bool:
        """
        키에 만료 시간 설정
        
        Args:
            key: 대상 키
            time: 만료 시간 (초)
            
        Returns:
            설정 성공 여부
        """
        try:
            return bool(self._client.expire(key, time))
        except Exception as e:
            logger.error(f"Redis EXPIRE 실패 - key: {key}, time: {time}, error: {e}")
            return False
    
    def check_connection(self) -> bool:
        """
        Redis 연결 상태 확인
        
        Returns:
            연결 상태 (True: 정상, False: 비정상)
        """
        try:
            self._client.ping()
            return True
        except Exception as e:
            logger.error(f"Redis 연결 확인 실패: {e}")
            return False
    
    def close(self):
        """Redis 연결 종료"""
        if self._client:
            self._client.close()
            logger.info("Redis 연결이 종료되었습니다.")


# 전역 데이터베이스 매니저 인스턴스
db_manager = DatabaseManager()
redis_manager = RedisManager()
