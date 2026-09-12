import os
from concurrent import futures
from dataclasses import dataclass

import grpc

import schema_registry_pb2
import schema_registry_pb2_grpc
from schema_registry_pb2 import Schema, Struct, Field 

 

class SchemaRegistryService(schema_registry_pb2_grpc.SchemaRegistryServicer):
    def __init__(self):
        self._registry: dict[str, list[Schema]] = {}

    def RegisterSchema(self, request, context): 
        if not request.service_name:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "service_name is empty")
        
        # 1. Валидация новой схемы
        issues = self._validate_schema(request.schema)
        
        versions = self._registry.get(request.service_name)
        
        # 2. Первая версия
        if versions is None:
            if issues:
                return schema_registry_pb2.RegisterSchemaResponse(
                    accepted=False, version=0, issues=issues,
                )
            self._registry[request.service_name] = [request.schema]
            return schema_registry_pb2.RegisterSchemaResponse(
                accepted=True, version=1, issues=[],
            )
        
        # 3. Сравнение с последней версией
        current_version = len(versions)
        latest = versions[-1]
        issues.extend(self._compare_schemas(latest, request.schema))
        
        if issues:
            return schema_registry_pb2.RegisterSchemaResponse(
                accepted=False, version=current_version, issues=issues,
            )
        
        versions.append(request.schema)
        return schema_registry_pb2.RegisterSchemaResponse(
            accepted=True, version=current_version + 1, issues=[],
        )

    def CheckCompatibility(self, request, context):
        if not request.service_name:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "service_name is empty")
        
        versions = self._registry.get(request.service_name)
        if versions is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "service not found")
        
        if request.base_version < 1 or request.base_version > len(versions):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid base_version")
        
        issues = self._validate_schema(request.candidate_schema)
        base_schema = versions[request.base_version - 1]
        issues.extend(self._compare_schemas(base_schema, request.candidate_schema))
        
        return schema_registry_pb2.CheckCompatibilityResponse(
            compatible=len(issues) == 0,
            issues=issues,
        )

    def GetSchema(self, request, context):
        versions = self._registry.get(request.service_name)
        if versions is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "service not found")
        if request.version < 1 or request.version > len(versions):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid version")
        return schema_registry_pb2.GetSchemaResponse(
            version=request.version,
            schema=versions[request.version - 1],
        )

    def GetLatestVersion(self, request, context):
        if not request.service_name:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "service_name is empty")
        if request.service_name not in self._registry:
            context.abort(grpc.StatusCode.NOT_FOUND, "service not found")
        return schema_registry_pb2.GetLatestVersionResponse(
            version=len(self._registry[request.service_name]),
        )

    def _make_issue(self, code, struct_name="", field_id=0, message=""):
        return schema_registry_pb2.CompatibilityIssue(
            code=code,
            struct_name=struct_name,
            field_id=field_id,
            message=message,
        )

    def _validate_schema(self, schema):
        issues = []
        seen_struct_names = set()
        
        for struct in schema.structs:
            if struct.name in seen_struct_names:
                issues.append(self._make_issue(
                    schema_registry_pb2.DUPLICATE_STRUCT_NAME,
                    struct_name=struct.name,
                    message=f"Duplicate struct name: {struct.name}",
                ))
            seen_struct_names.add(struct.name)
            
            seen_ids = set()
            seen_names = set()
            for field in struct.fields:
                if field.id in seen_ids:
                    issues.append(self._make_issue(
                        schema_registry_pb2.DUPLICATE_FIELD_ID,
                        struct_name=struct.name,
                        field_id=field.id,
                        message=f"Duplicate field id {field.id} in struct {struct.name}",
                    ))
                if field.name in seen_names:
                    issues.append(self._make_issue(
                        schema_registry_pb2.DUPLICATE_FIELD_NAME,
                        struct_name=struct.name,
                        field_id=field.id,
                        message=f"Duplicate field name '{field.name}' in struct {struct.name}",
                    ))
                seen_ids.add(field.id)
                seen_names.add(field.name)
        
        return issues

    def _compare_structs(self, old_struct, new_struct):
        issues = []
        
        old_by_id = {f.id: f for f in old_struct.fields}
        new_by_id = {f.id: f for f in new_struct.fields}
        old_by_name = {f.name: f for f in old_struct.fields}
        new_by_name = {f.name: f for f in new_struct.fields}
        
        # 1. Поля с одинаковым id: тип, required
        for fid, old_f in old_by_id.items():
            if fid not in new_by_id:
                continue
            new_f = new_by_id[fid]
            
            if old_f.type != new_f.type:
                issues.append(self._make_issue(
                    schema_registry_pb2.FIELD_TYPE_CHANGED,
                    struct_name=new_struct.name,
                    field_id=fid,
                    message=f"Field {fid} type changed from {old_f.type} to {new_f.type}",
                ))
            
            if not old_f.required and new_f.required:
                issues.append(self._make_issue(
                    schema_registry_pb2.OPTIONAL_TO_REQUIRED,
                    struct_name=new_struct.name,
                    field_id=fid,
                    message=f"Field {fid} changed from optional to required",
                ))
        
        # 2. Новые поля (id есть в new, нет в old)
        for fid, new_f in new_by_id.items():
            if fid not in old_by_id and new_f.required:
                issues.append(self._make_issue(
                    schema_registry_pb2.ADDED_REQUIRED_FIELD,
                    struct_name=new_struct.name,
                    field_id=fid,
                    message=f"Added required field {fid}",
                ))
        
        # 3. Удалённые поля (id есть в old, нет в new)
        for fid, old_f in old_by_id.items():
            if fid not in new_by_id and old_f.required:
                issues.append(self._make_issue(
                    schema_registry_pb2.REMOVED_REQUIRED_FIELD,
                    struct_name=new_struct.name,
                    field_id=fid,
                    message=f"Removed required field {fid}",
                ))
        
        # 4. Смена id у поля с тем же name
        for name, old_f in old_by_name.items():
            if name in new_by_name:
                new_f = new_by_name[name]
                if old_f.id != new_f.id:
                    issues.append(self._make_issue(
                        schema_registry_pb2.FIELD_ID_CHANGED,
                        struct_name=new_struct.name,
                        field_id=new_f.id,
                        message=f"Field '{name}' changed id from {old_f.id} to {new_f.id}",
                    ))
        
        return issues

    def _compare_schemas(self, old_schema, new_schema):
        issues = []
        old_by_name = {s.name: s for s in old_schema.structs}
        new_by_name = {s.name: s for s in new_schema.structs}
        
        for name, new_struct in new_by_name.items():
            if name in old_by_name:
                issues.extend(self._compare_structs(old_by_name[name], new_struct))
            # новый struct — разрешён, ничего не делаем
        
        return issues


def serve():
    port = int(os.getenv("PORT", "50051"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    schema_registry_pb2_grpc.add_SchemaRegistryServicer_to_server(
        SchemaRegistryService(),
        server,
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()