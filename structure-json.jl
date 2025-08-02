using Pkg

if length(ARGS) == 0
    println("Cannot run without a file")
    exit(1)
end

function ensure_package(pkgs::String)
    for pkg in split(pkgs, " ")
        try
            # The `@eval` macro is used to run a command built from a string
            @eval using $(Symbol(pkg))
        catch e
            if e isa ArgumentError
                println("Package '$(pkg)' not found. Installing...")
                Pkg.add(pkg)
                @eval using $(Symbol(pkg))
                println("Package '$(pkg)' is now loaded.")
            else
                # Re-throw other errors
                rethrow(e)
            end
        end
    end
end

ensure_package("JSON DataFrames JSON3 JSONTables CSV")

function jsonToDataFrame(data::String)::DataFrame
    return jsontable(data) |> DataFrame
end

function extractMatches(text::String, messages::Vector{String})::Vector{Union{Nothing, RegexMatch{String}}}
    return match.(r"\"" * text * r"\": (\{.+?\})", messages)
end

function extractJsonString(regexMatch::Union{Nothing, RegexMatch{String}})::Union{Nothing, String}
    if isnothing(regexMatch)
        return nothing
    else
        return "[" * string(regexMatch.captures[1]) * "]"
    end
end

function extractJsonString(regexMatches::Vector{Union{Nothing, RegexMatch{String}}})::Vector{Union{Nothing, String}}
    return map(extractJsonString, regexMatches)
end

function formatDataFrame(jsonString::Union{Nothing, String})::DataFrame
    if isnothing(jsonString)
        return DataFrame()
    else
        return jsontable(jsonString) |> DataFrame
    end
end

function formatDataFrame(jsonStrings::Vector{Union{Nothing, String}})::Vector{DataFrame}
    return map(formatDataFrame, jsonStrings)
end

function concatDataFrames(dataFrames::Vector{DataFrame})::DataFrame
    allNames = mapreduce(names, vcat, dataFrames) |> unique
    return mapreduce(
        df -> isempty(df) ? DataFrame([colName => missing for colName in allNames]) : df,
        vcat,
        dataFrames
    )
end

function extendedDataFrame(df::DataFrame, text::String, messagesColumn::Symbol)::DataFrame
    traceDetail = extractMatches(text, df[!, messagesColumn]) |> extractJsonString |> formatDataFrame |> concatDataFrames
    return hcat(df, traceDetail)
end

function jsonToCsv(args::Vector{String})
    println("Starting JSON to CSV conversion detecting embedded JSON structure...")
    keyToExtract = "trace_detail"
    inputFile = args[1]
    if length(args) == 2
        inputFile, keyToExtract = args
    end
    println("Using $(keyToExtract) as placeholder to identify embedded JSON structure...")
    filename = replace(inputFile, ".json" => ".csv")
    println("Reading file: ", inputFile)
    df = read(inputFile, String) |> jsonToDataFrame
    println("Processing data...")
    dfExtended = extendedDataFrame(df, keyToExtract, :message)
    println("Writing output CSV to file: ", filename)
    CSV.write(filename, dfExtended)
    println("File has been written. Goodbye.")
end

jsonToCsv(ARGS)