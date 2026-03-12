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

    async def run(self):
        """Executes the load in the correct order with SK resolution."""
        load_order = self._get_load_order()
        
        for model in load_order:
            data, key_map = self._registry[model]
            
            if issubclass(model, DimensionModel):
                track_cols = [
                    name for name in model.model_fields.keys() 
                    if name not in ["id", "valid_from", "valid_to", "is_current", "checksum"]
                ]
                handler = SCDHandler(model, self.session, track_columns=track_cols)
                await handler.process(data)
            else:
                # Fact Load with SK Resolution
                processed_data = []
                for record in data:
                    processed_record = record.copy()
                    
                    # Resolve natural keys to surrogate keys
                    for fk_col, (dim_model, nk_name) in key_map.items():
                        nk_value = record.get(fk_col)
                        sk_value = self.resolver.resolve(dim_model, nk_name, nk_value)
                        if sk_value is not None:
                            processed_record[fk_col] = sk_value
                        else:
                            # In a real system, we'd log this or handle "Inferred Members"
                            pass
                            
                    processed_data.append(processed_record)

                # Execute based on bound strategy
                strategy_name = getattr(model, "__strategy__", None)
                if strategy_name == "bulk":
                    from sqldim.loaders.strategies import BulkInsertStrategy
                    BulkInsertStrategy().execute(self.session, model, processed_data)
                elif strategy_name == "upsert":
                    from sqldim.loaders.strategies import UpsertStrategy
                    # Assumes natural key is the conflict target
                    nk = getattr(model, "__natural_key__", ["id"])[0]
                    UpsertStrategy(conflict_column=nk).execute(self.session, model, processed_data)
                elif strategy_name == "merge":
                    from sqldim.loaders.strategies import MergeStrategy
                    nk = getattr(model, "__natural_key__", ["id"])[0]
                    MergeStrategy(match_column=nk).execute(self.session, model, processed_data)
                elif strategy_name == "accumulating":
                    from sqldim.loaders.accumulating import AccumulatingLoader
                    # Expects specific metadata for accumulating facts
                    match_col = getattr(model, "__match_column__", "id")
                    milestones = getattr(model, "__milestones__", [])
                    loader = AccumulatingLoader(model, match_col, milestones, self.session)
                    loader.process(processed_data)
                else:
                    # Default: standard loop
                    for row_data in processed_data:
                        self.session.add(model(**row_data))
                    self.session.commit()
