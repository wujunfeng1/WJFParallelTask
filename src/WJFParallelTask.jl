module WJFParallelTask
export mapPrefix, mapReduce
using Distributed

function mapPrefix(data::T2, batchSize::Int,
    mapFun::Function, blockPrefixFun::Function;
    globalStates::Dict{String,Any} = Dict{String,Any}(),
    attachments::Vector = [],
    copyData::Bool = false,
    )::T2 where {T2<:Vector}
    if length(attachments) > 0
        @assert length(attachments) == length(data)
    end
    numWorkers = length(workers())
    jobs = RemoteChannel(() -> Channel{Tuple{Int,Int,T2,Dict{String,Any}}}(numWorkers))
    jobOutputs = RemoteChannel(() -> Channel{Vector{Tuple{Int,Int,T2}}}(numWorkers))

    function makeJobs()
        for i = 1:batchSize:length(data)
            iEnd = min(i + batchSize - 1, length(data))
            segment = if copyData
                if length(attachments) == length(data)
                    [(data[j],attachments[j]) for j in 1:length(data)]
                else
                    data
                end
            else
                if length(attachments) == length(data)
                    [(data[j],attachments[j]) for j in i:iEnd]
                else
                    data[i:iEnd]
                end
            end
            put!(jobs, (i, iEnd, segment, globalStates))
        end
        for idxWorker = 1:numWorkers
            put!(jobs, (0,0,T2(),Dict{String,Any}()))
        end
    end

    errormonitor(@async makeJobs())
    for p in workers()
        remote_do((jobs, jobOutputs) -> begin
            localResult::Vector{Tuple{Int,Int,T2}} = Vector{Tuple{Int,Int,T2}}()
            job = take!(jobs)
            while length(job[3]) > 0
                push!(localResult, (job[1], job[2],
                    mapFun(job[1], job[2], job[3], job[4])))
                job = take!(jobs)
            end
            put!(jobOutputs, localResult)
        end, p, jobs, jobOutputs)
    end

    localResults::Vector{Tuple{Int,Int,T2}} = Vector{Tuple{Int,Int,T2}}()
    for idxWorker = 1:numWorkers
        append!(localResults, take!(jobOutputs))
    end
    sort!(localResults, by=x->x[1])
    result = T2()
    for block in localResults
        if length(result) == 0
            append!(result, block[3])
        else
            append!(result, blockPrefixFun(result[end], block[3]))
        end
    end
    return result
end

function mapReduce(data::T2, batchSize::Int,
    mapFun::Function, reduceFun::Function,
    outData0::T3;
    globalStates::Dict{String,Any} = Dict{String,Any}(),
    attachments::Vector = [],
    copyData::Bool = false,
    )::T3 where {T2<:Vector, T3<:Any}
    if length(attachments) > 0
        @assert length(attachments) == length(data)
    end
    numWorkers = length(workers())
    jobs = RemoteChannel(() -> Channel{Tuple{Int,Int,T2,Dict{String,Any}}}(numWorkers))
    jobOutputs = RemoteChannel(() -> Channel{T3}(numWorkers))

    function makeJobs()
        for i = 1:batchSize:length(data)
            iEnd = min(i + batchSize - 1, length(data))
            segment = if copyData
                if length(attachments) == length(data)
                    [(data[j],attachments[j]) for j in 1:length(data)]
                else
                    data
                end
            else
                if length(attachments) == length(data)
                    [(data[j],attachments[j]) for j in i:iEnd]
                else
                    data[i:iEnd]
                end
            end
            put!(jobs, (i, iEnd, segment, globalStates))
        end
        for idxWorker = 1:numWorkers
            put!(jobs, (0,0,T2(),Dict{String,Any}()))
        end
    end

    errormonitor(@async makeJobs())
    for p in workers()
        remote_do((jobs, jobOutputs) -> begin
            localResult::Vector{T3} = Vector{T3}()
            job = take!(jobs)
            while length(job[3]) > 0
                push!(localResult, mapFun(job[1], job[2], job[3], job[4]))
                job = take!(jobs)
            end
            put!(jobOutputs, reduceFun(localResult))
        end, p, jobs, jobOutputs)
    end

    localResults = T3[outData0]
    for idxWorker = 1:numWorkers
        push!(localResults, take!(jobOutputs))
    end
    return reduceFun(localResults)
end

end # module
