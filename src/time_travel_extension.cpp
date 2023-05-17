#define DUCKDB_EXTENSION_MAIN

#include "time_travel_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"


#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void Time_travelScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) { 
			return StringVector::AddString(result, "Time_travel "+name.GetString()+" 🐥");;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
	Connection con(instance);
    con.BeginTransaction();

    auto &catalog = Catalog::GetSystemCatalog(*con.context);

    CreateScalarFunctionInfo time_travel_fun_info(
            ScalarFunction("time_travel", {LogicalType::VARCHAR}, LogicalType::VARCHAR, Time_travelScalarFun));
    time_travel_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &time_travel_fun_info);
    con.Commit();
}

void Time_travelExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string Time_travelExtension::Name() {
	return "time_travel";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void time_travel_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *time_travel_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
