#define DUCKDB_EXTENSION_MAIN
#include <iostream>

#include "time_travel_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/parser/parser.hpp"

#include "re2/re2.h"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb
{

    inline void bpsUDF(DataChunk &args, ExpressionState &state, Vector &result)
    {
        auto &name_vector = args.data[0];
        UnaryExecutor::Execute<string_t, string_t>(
            name_vector, result, args.size(),
            [&](string_t name)
            {
                return StringVector::AddString(result, "bps " + name.GetString() + " 🐥");
                ;
            });
    }

    ParserExtensionParseResult parse(ParserExtensionInfo *,
                                     const std::string &query)
    {
        long as_of;
        RE2::PartialMatch(query, "WHEN as-of=(\\d+)?", &as_of);
        std::cout << "EDWIN " + as_of << "\n";

        string udf_call = "bps('" + std::to_string(as_of) + "')";

        std::string query_copy(query);
        RE2::Replace(&query_copy, "WHEN as-of=\\d+", "");
        RE2::Replace(&query_copy, "bps", udf_call);
        std::cout << "EDWIN " + udf_call << "\n";

        Parser parser;
        parser.ParseQuery(std::move(query_copy));
        vector<unique_ptr<SQLStatement>> statements = std::move(parser.statements);
        return ParserExtensionParseResult(
            make_unique_base<ParserExtensionParseData, WhenParseData>(
                std::move(statements[0])));
    }

    ParserExtensionPlanResult plan(ParserExtensionInfo *, ClientContext &context,
                                   unique_ptr<ParserExtensionParseData> parse_data)
    {
        auto state = make_shared<QueryState>(std::move(parse_data));
        context.registered_state["TimeTravel"] = state;
        throw BinderException("Use psql_bind instead");
    }

    BoundStatement query_bind(ClientContext &context, Binder &binder,
                              OperatorExtensionInfo *info, SQLStatement &statement)
    {
        switch (statement.type)
        {
        case StatementType::EXTENSION_STATEMENT:
        {
            auto &extension_statement = dynamic_cast<ExtensionStatement &>(statement);
            if (extension_statement.extension.parse_function == parse)
            {
                auto lookup = context.registered_state.find("TimeTravel");
                if (lookup != context.registered_state.end())
                {
                    auto psql_state = (QueryState *)lookup->second.get();
                    auto psql_binder = Binder::CreateBinder(context);
                    auto psql_parse_data =
                        dynamic_cast<WhenParseData *>(psql_state->parse_data.get());
                    return psql_binder->Bind(*(psql_parse_data->statement));
                }
                throw BinderException("Registered state not found");
            }
        }
        default:
            // No-op empty
            return {};
        }
    }

    static void LoadInternal(DatabaseInstance &instance)
    {
        // Parser
        auto &config = DBConfig::GetConfig(instance);
        WhenParserExtension when_parser;
        config.parser_extensions.push_back(when_parser);
        config.operator_extensions.push_back(make_unique<TimeTravelOperatorExtension>());

        // UDF creation
        Connection con(instance);
        con.BeginTransaction();

        auto &catalog = Catalog::GetSystemCatalog(*con.context);

        CreateScalarFunctionInfo time_travel_fun_info(
            ScalarFunction("bps", {LogicalType::VARCHAR}, LogicalType::VARCHAR, bpsUDF));
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
