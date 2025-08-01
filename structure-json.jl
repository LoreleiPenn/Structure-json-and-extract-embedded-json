using JSON, DataFrames, JSON3, JSONTables

inputFile = "log.json"

function jsonToDataFrame(data::String)::DataFrame
    return jsontable(data) |> DataFrame
end

df = read(inputFile, String) |> jsonToDataFrame

df[!, :message]

matches = match.(r"\"trace_detail\": (\{.+?\})", df[!, :message])

function extractJsonString(regexMatch::Union{Nothing, RegexMatch{String}})::Union{Nothing, String}
    if regexMatch === nothing
        return nothing
    else
        return "[" * string(regexMatch.captures[1]) * "]"
    end
end

function extractJsonString(regexMatches::Vector{Union{Nothing, RegexMatch{String}}})::Vector{Union{Nothing, String}}
    return map(extractJsonString, regexMatches)
end

function formatDataFrame(jsonString::Union{Nothing, String})::DataFrame
    if jsonString === nothing
        return DataFrame()
    else
        return jsontable(jsonString) |> DataFrame
    end
end

function formatDataFrame(jsonStrings::Vector{Union{Nothing, String}})::Vector{DataFrame}
    return map(formatDataFrame, jsonStrings)
end

function concatDataFrames(dataFrames::Vector{DataFrame})::DataFrame
    allNames = mapreduce(names, vcat, dataFrames) |> Set
    
end

dfs = extractJsonString(matches) |> formatDataFrame