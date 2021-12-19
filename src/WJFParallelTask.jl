module WJFParallelTask
export mapPrefix, mapReduce
using Distributed

@everywhere function mapPrefix(loopStart::T1, loopEnd::T1, batchSize::T1,
    mapFun::Function, blockPrefixFun::Function,
    data::T2,
    copyData::Bool = false,
    )::T2 where {T1<:Integer, T2<:Vector}
    numWorkers = length(workers())
    jobs = RemoteChannel(() -> Channel{Tuple{T1,T1,T2}}(numWorkers))
    jobOutputs = RemoteChannel(() -> Channel{Vector{Tuple{T1,T1,T2}}}(numWorkers))

    function makeJobs()
        for i::T1 = loopStart:batchSize:loopEnd
            iEnd = min(i + batchSize - 1, loopEnd)
            if copyData
                put!(jobs, (i, iEnd, data))
            else
                put!(jobs, (i, iEnd, data[i:iEnd]))
            end
        end
        for idxWorker = 1:numWorkers
            put!(jobs, (loopEnd,loopEnd,T2()))
        end
    end

    errormonitor(@async makeJobs())
    for p in workers()
        remote_do((jobs, jobOutputs) -> begin
            localResult::Vector{Tuple{T1,T1,T2}} = Vector{Tuple{T1,T1,T2}}()
            job = take!(jobs)
            while length(job[3]) > 0
                push!(localResult, (job[1], job[2], mapFun(job[1], job[2], job[3])))
                job = take!(jobs)
            end
            put!(jobOutputs, localResult)
        end, p, jobs, jobOutputs)
    end

    localResults::Vector{Tuple{T1,T1,T2}} = Vector{Tuple{T1,T1,T2}}()
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

@everywhere function mapReduce(loopStart::T1, loopEnd::T1, batchSize::T1,
    mapFun::Function, reduceFun::Function,
    data::T2, outData0::T3,
    copyData::Bool = false,
    )::T3 where {T1<:Integer, T2<:Vector, T3<:Any}
    numWorkers = length(workers())
    jobs = RemoteChannel(() -> Channel{Tuple{T1,T1,T2}}(numWorkers))
    jobOutputs = RemoteChannel(() -> Channel{T3}(numWorkers))

    function makeJobs()
        for i::T1 = loopStart:batchSize:loopEnd
            iEnd = min(i + batchSize - 1, loopEnd)
            if copyData
                put!(jobs, (i, iEnd, data))
            else
                put!(jobs, (i, iEnd, data[i:iEnd]))
            end
        end
        for idxWorker = 1:numWorkers
            put!(jobs, (loopEnd,loopEnd,T2()))
        end
    end

    errormonitor(@async makeJobs())
    for p in workers()
        remote_do((jobs, jobOutputs) -> begin
            localResult::Vector{T3} = Vector{T3}()
            job = take!(jobs)
            while length(job[3]) > 0
                push!(localResult, mapFun(job[1], job[2], job[3]))
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
