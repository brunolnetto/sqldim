from typing import Any, Dict, List, Type, Optional, Tuple
from sqlmodel import Session, select
from sqldim.core.graph import SchemaGraph
from sqldim.core.models import DimensionModel, FactModel
from sqldim.scd.handler import SCDHandler

class SKResolver:
    def __init__(self, session: Session):
        self.session = session
        self._cache: Dict[Tuple[Type[DimensionModel], str, Any], Any] = {}

    def resolve(self, model: Type[DimensionModel], natural_key_name: str, value: Any) -> Any:
        """Resolves a natural key value to a surrogate key (id)."""
        cache_key = (model, natural_key_name, value)
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Query the current version of the dimension row
        stmt = select(model.id).where(
            getattr(model, natural_key_name) == value,
            getattr(model, "is_current") == True
        )
        sk = self.session.exec(stmt).first()
        
        if sk is not None:
            self._cache[cache_key] = sk
        return sk

class DimensionalLoader:
    def __init__(self, session: Session, models: List[Type[Any]]):
        self.session = session
        self.graph = SchemaGraph.from_models(models)
        self._registry: Dict[Type[Any], Tuple[List[Dict[str, Any]], Dict[str, Tuple[Type[DimensionModel], str]]]] = {}
        self.resolver = SKResolver(session)

    def register(
        self, 
        model: Type[Any], 
        source: List[Dict[str, Any]], 
        key_map: Optional[Dict[str, Tuple[Type[DimensionModel], str]]] = None
    ):
        """
        Register a model and its source data for loading.
        key_map: For facts, maps {fact_fk_column: (DimensionModel, natural_key_name)}
        """
        self._registry[model] = (source, key_map or {})

    def _get_load_order(self) -> List[Type[Any]]:
        dims = [m for m in self._registry.keys() if issubclass(m, DimensionModel)]
        facts = [m for m in self._registry.keys() if issubclass(m, FactModel)]
        return dims + facts

    async def _load_dimension(self, model: Type, data: list) -> None:
        track_cols = [
            name for name in model.model_fields.keys()
            if name not in ["id", "valid_from", "valid_to", "is_current", "checksum"]
        ]
        handler = SCDHandler(model, self.session, track_columns=track_cols)
        await handler.process(data)

    def _resolve_fks(self, record: dict, key_map: dict) -> dict:
        processed = record.copy()
        for fk_col, (dim_model, nk_name) in key_map.items():
            sk_value = self.resolver.resolve(dim_model, nk_name, record.get(fk_col))
            if sk_value is not None:
                processed[fk_col] = sk_value
        return processed

    def _insert_all(self, model: Type, records: list) -> None:
        for row_data in records:
            self.session.add(model(**row_data))
        self.session.commit()

    async def _execute_fact_strategy(self, model: Type, strategy_name: Optional[str], processed_data: list, key_map: dict) -> None:
        nk = getattr(model, "__natural_key__", ["id"])[0]
        if strategy_name == "bulk":
            from sqldim.loaders.strategies import BulkInsertStrategy
            BulkInsertStrategy().execute(self.session, model, processed_data)
        elif strategy_name == "upsert":
            from sqldim.loaders.strategies import UpsertStrategy
            UpsertStrategy(conflict_column=nk).execute(self.session, model, processed_data)
        elif strategy_name == "merge":
            from sqldim.loaders.strategies import MergeStrategy
            MergeStrategy(match_column=nk).execute(self.session, model, processed_data)
        elif strategy_name == "accumulating":
            from sqldim.loaders.accumulating import AccumulatingLoader
            loader = AccumulatingLoader(model, getattr(model, "__match_column__", "id"), getattr(model, "__milestones__", []), self.session)
            loader.process(processed_data)
        else:
            self._insert_all(model, processed_data)

    async def run(self):
        """Executes the load in the correct order with SK resolution."""
        for model in self._get_load_order():
            data, key_map = self._registry[model]
            if issubclass(model, DimensionModel):
                await self._load_dimension(model, data)
            else:
                processed_data = [self._resolve_fks(r, key_map) for r in data]
                await self._execute_fact_strategy(model, getattr(model, "__strategy__", None), processed_data, key_map)
