#pragma once

#include "duckdb.hpp"

namespace duckdb
{

  class Time_travelExtension : public Extension
  {
  public:
    void Load(DuckDB &db) override;
    std::string Name() override;
  };

  ParserExtensionParseResult when_parse(ParserExtensionInfo *,
                                        const std::string &query);

  // ParserExtensionPlanResult psql_plan(ParserExtensionInfo *, ClientContext &,
  //                                     unique_ptr<ParserExtensionParseData>);

  struct WhenParserExtension : public ParserExtension
  {
    WhenParserExtension() : ParserExtension()
    {
      parse_function = when_parse;
      // plan_function = psql_plan;
    }
  };

  struct WhenParseData : ParserExtensionParseData
  {
    unique_ptr<SQLStatement> statement;

    unique_ptr<ParserExtensionParseData> Copy() const override
    {
      return make_unique_base<ParserExtensionParseData, WhenParseData>(
          statement->Copy());
    }

    WhenParseData(unique_ptr<SQLStatement> statement)
        : statement(std::move(statement)) {}
  };

} // namespace duckdb
