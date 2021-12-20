module WJFParallelTask
export mapPrefix, mapReduce
using Distributed

function mapPrefix(data::T1, batchSize::Int,
    mapFun::Function, blockPrefixFun::Function,
    outData0::T2;
    globalStates::Dict{String,Any} = Dict{String,Any}(),
    attachments::Vector = [],
    copyData::Bool = false,
    )::T2 where {T1<:Vector,T2<:Any}
    if length(attachments) > 0
        @assert length(attachments) == length(data)
    end
    numWorkers = length(workers())
    jobs = RemoteChannel(() ->
        Channel{Tuple{Int,Int,T1,Dict{String,Any}}}(numWorkers))
    jobOutputs = RemoteChannel(() ->
        Channel{Vector{Tuple{Int,Int,Vector{T2}}}}(numWorkers))

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
            put!(jobs, (0,0,T1(),Dict{String,Any}()))
        end
    end

    errormonitor(@async makeJobs())
    for p in workers()
        remote_do((jobs, jobOutputs) -> begin
            localResult::Vector{Tuple{Int,Int,Vector{T2}}} =
                Vector{Tuple{Int,Int,Vector{T2}}}()
            job = take!(jobs)
            while length(job[3]) > 0
                push!(localResult, (job[1], job[2],
                    mapFun(job[1], job[2], job[3], job[4])))
                job = take!(jobs)
            end
            put!(jobOutputs, localResult)
        end, p, jobs, jobOutputs)
    end

    localResults::Vector{Tuple{Int,Int,Vector{T2}}} =
        Vector{Tuple{Int,Int,Vector{T2}}}()
    for idxWorker = 1:numWorkers
        append!(localResults, take!(jobOutputs))
    end
    sort!(localResults, by=x->x[1])
    result = T2[]
    for block in localResults
        if length(result) == 0
            append!(result, blockPrefixFun(outputData0,block[3]))
        else
            append!(result, blockPrefixFun(result[end], block[3]))
        end
    end
    return result
end

function mapReduce(data::T1, batchSize::Int,
    mapFun::Function, reduceFun::Function,
    outData0::T2;
    globalStates::Dict{String,Any} = Dict{String,Any}(),
    attachments::Vector = [],
    copyData::Bool = false,
    )::T2 where {T1<:Vector, T2<:Any}
    if length(attachments) > 0
        @assert length(attachments) == length(data)
    end
    numWorkers = length(workers())
    jobs = RemoteChannel(() -> Channel{Tuple{Int,Int,T1,Dict{String,Any}}}(numWorkers))
    jobOutputs = RemoteChannel(() -> Channel{T2}(numWorkers))

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
            put!(jobs, (0,0,T1(),Dict{String,Any}()))
        end
    end

    errormonitor(@async makeJobs())
    for p in workers()
        remote_do((jobs, jobOutputs) -> begin
            localResult::Vector{T2} = Vector{T2}()
            job = take!(jobs)
            while length(job[3]) > 0
                push!(localResult, mapFun(job[1], job[2], job[3], job[4]))
                job = take!(jobs)
            end
            put!(jobOutputs, reduceFun(localResult))
        end, p, jobs, jobOutputs)
    end

    localResults = T2[outData0]
    for idxWorker = 1:numWorkers
        push!(localResults, take!(jobOutputs))
    end
    return reduceFun(localResults)
end

end # module
