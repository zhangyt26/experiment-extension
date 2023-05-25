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

  ParserExtensionParseResult parse(ParserExtensionInfo *,
                                   const std::string &query);

  ParserExtensionPlanResult plan(ParserExtensionInfo *, ClientContext &,
                                 unique_ptr<ParserExtensionParseData>);

  struct WhenParserExtension : public ParserExtension
  {
    WhenParserExtension() : ParserExtension()
    {
      parse_function = parse;
      plan_function = plan;
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

  class QueryState : public ClientContextState
  {
  public:
    explicit QueryState(unique_ptr<ParserExtensionParseData> parse_data)
        : parse_data(std::move(parse_data)) {}

    void QueryEnd() override { parse_data.reset(); }

    unique_ptr<ParserExtensionParseData> parse_data;
  };

  BoundStatement query_bind(ClientContext &context, Binder &binder,
                            OperatorExtensionInfo *info, SQLStatement &statement);

  struct TimeTravelOperatorExtension : public OperatorExtension
  {
    TimeTravelOperatorExtension() : OperatorExtension() { Bind = query_bind; }

    std::string GetName() override { return "TimeTravel"; }

    unique_ptr<LogicalExtensionOperator>
    Deserialize(LogicalDeserializationState &state,
                FieldReader &reader) override
    {
      throw InternalException("psql operator should not be serialized");
    }
  };

} // namespace duckdb
