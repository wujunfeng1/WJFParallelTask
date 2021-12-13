module WJFParallelTask
export mapPrefix, mapReduce, mapOnly

function mapPrefix(loopStart::T1, loopEnd::T1, batchSize::T1,
    mapFun::Function, blockPrefixFun::Function,
    initialResult::T2, parallel::Bool = true
    )::T2 where {T1<:Integer, T2<:Vector}
    numCPUs = length(Sys.cpu_info())
    jobs = Channel{Tuple{T1,T1}}(numCPUs)
    jobOutputs = Channel{Vector{Tuple{T1,T1,T2}}}(numCPUs)

    function makeJobs()
        for i::T1 = loopStart:batchSize:loopEnd
            put!(jobs, (i, min(i + batchSize - 1, loopEnd)))
        end
    end

    function runJob(iCPU::Int)
        localResult::Vector{Tuple{T1,T1,T2}} = Vector{Tuple{T1,T1,T2}}()
        for job in jobs
            push!(localResult, (job[1], job[2], mapFun(job[1], job[2])))
        end # job
        put!(jobOutputs, localResult)
    end # runJob

    bind(jobs, @async makeJobs())
    for iCPU = 1:numCPUs
        if parallel
            Threads.@spawn runJob(iCPU)
        else
            runJob(iCPU)
        end
    end

    localResults::Vector{Tuple{T1,T1,T2}} = Vector{Tuple{T1,T1,T2}}()
    for iCPU = 1:numCPUs
        append!(localResults, take!(jobOutputs))
    end
    sort!(localResults, by=x->x[1])
    result = initialResult
    for block in localResults
        if length(result) == 0
            append!(result, block[3])
        else
            append!(result, blockPrefixFun(result[end], block[3]))
        end
    end
    return result
end

function mapReduce(loopStart::T1, loopEnd::T1, batchSize::T1,
    mapFun::Function, reduceFun::Function,
    x0::T2, parallel::Bool = true
    )::T2 where {T1<:Integer, T2<:Any}
    numCPUs = length(Sys.cpu_info())
    jobs = Channel{Tuple{T1,T1}}(numCPUs)
    jobOutputs = Channel{T2}(numCPUs)

    function makeJobs()
        for i::T1 = loopStart:batchSize:loopEnd
            put!(jobs, (i, min(i + batchSize - 1, loopEnd)))
        end
    end

    function runJob(iCPU::Int)
        localResult::Vector{T2} = Vector{T2}()
        for job in jobs
            push!(localResult, mapFun(job[1], job[2]))
        end # job
        put!(jobOutputs, reduceFun(localResult))
    end # runJob

    bind(jobs, @async makeJobs())
    for iCPU = 1:numCPUs
        if parallel
            Threads.@spawn runJob(iCPU)
        else
            runJob(iCPU)
        end
    end

    localResults = T2[x0]
    for iCPU = 1:numCPUs
        push!(localResults, take!(jobOutputs))
    end
    return reduceFun(localResults)
end

function mapOnly(loopStart::T1, loopEnd::T1, batchSize::T1,
    mapFun::Function, parallel::Bool = true
    ) where T1<:Integer
    numCPUs = length(Sys.cpu_info())
    jobs = Channel{Tuple{T1,T1}}(numCPUs)
    jobOutputs = Channel{Bool}(numCPUs)

    function makeJobs()
        for i::T1 = loopStart:batchSize:loopEnd
            put!(jobs, (i, min(i + batchSize - 1, loopEnd)))
        end
    end

    function runJob(iCPU::Int)
        for job in jobs
            mapFun(job[1], job[2])
        end # job
        put!(jobOutputs, true)
    end # runJob

    bind(jobs, @async makeJobs())
    for iCPU = 1:numCPUs
        if parallel
            Threads.@spawn runJob(iCPU)
        else
            runJob(iCPU)
        end
    end

    for iCPU = 1:numCPUs
        take!(jobOutputs)
    end
end

end # module
