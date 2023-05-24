#define DUCKDB_EXTENSION_MAIN

#include "time_travel_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parser.hpp"

#include "re2/re2.h"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb
{

    template <typename... Args>
    std::string string_format(const std::string &format, Args... args)
    {
        int size_s = std::snprintf(nullptr, 0, format.c_str(), args...) + 1; // Extra space for '\0'
        if (size_s <= 0)
        {
            throw std::runtime_error("Error during formatting.");
        }
        auto size = static_cast<size_t>(size_s);
        std::unique_ptr<char[]> buf(new char[size]);
        std::snprintf(buf.get(), size, format.c_str(), args...);
        return std::string(buf.get(), buf.get() + size - 1); // We don't want the '\0' inside
    }

    inline void Time_travelScalarFun(DataChunk &args, ExpressionState &state, Vector &result)
    {
        auto &name_vector = args.data[0];
        UnaryExecutor::Execute<string_t, string_t>(
            name_vector, result, args.size(),
            [&](string_t name)
            {
                return StringVector::AddString(result, "Time_travel " + name.GetString() + " 🐥");
                ;
            });
    }

    ParserExtensionParseResult when_parse(ParserExtensionInfo *,
                                          const std::string &query)
    {
        std::stringstream ss;

        string as_of;
        RE2::FullMatch(query, "WHEN as-of=(\\w+)", &as_of);

        string udf_call = string_format("Time_travel('')", as_of);
        duckdb_re2::StringPiece input(udf_call);

        RE2::Replace(query, "b+", "d");

        Parser parser;
        parser.ParseQuery(std::move(result));
        vector<unique_ptr<SQLStatement>> statements = std::move(parser.statements);
        return ParserExtensionParseResult(
            make_unique_base<ParserExtensionParseData, WhenParseData>(
                std::move(statements[0])));
    }

    static void LoadInternal(DatabaseInstance &instance)
    {
        // Parser
        auto &config = DBConfig::GetConfig(instance);
        WhenParserExtension when_parser;
        config.parser_extensions.push_back(when_parser);

        // UDF creation
        Connection con(instance);
        con.BeginTransaction();

        auto &catalog = Catalog::GetSystemCatalog(*con.context);

        CreateScalarFunctionInfo time_travel_fun_info(
            ScalarFunction("time_travel", {LogicalType::VARCHAR}, LogicalType::VARCHAR, Time_travelScalarFun));
        time_travel_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        catalog.CreateFunction(*con.context, &time_travel_fun_info);
        con.Commit();
    }

    void Time_travelExtension::Load(DuckDB &db)
    {
        LoadInternal(*db.instance);
    }
    std::string Time_travelExtension::Name()
    {
        return "time_travel";
    }

} // namespace duckdb

extern "C"
{

    DUCKDB_EXTENSION_API void time_travel_init(duckdb::DatabaseInstance &db)
    {
        LoadInternal(db);
    }

    DUCKDB_EXTENSION_API const char *time_travel_version()
    {
        return duckdb::DuckDB::LibraryVersion();
    }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
