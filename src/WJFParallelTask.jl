module WJFParallelTask
export mapPrefix, mapReduce, mapOnly

function mapPrefix(
    loopStart::T1,
    loopEnd::T1,
    batchSize::T1,
    mapFun::Function,
    blockPrefixFun::Function,
    initialResult::T2,
    parallel::Bool = true,
)::T2 where {T1<:Integer,T2<:Vector}
    if parallel && Threads.threadid() == 1
        numCPUs = length(Sys.cpu_info())
        jobs = Channel{Tuple{T1,T1}}(numCPUs)
        jobOutputs = Vector{Vector{Tuple{T1,T1,T2}}}(undef, numCPUs)
        jobFlags = fill(false, numCPUs)

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
            jobOutputs[iCPU] = localResult
            yield()
            jobFlags[iCPU] = true
        end # runJob

        bind(jobs, @async makeJobs())
        begin
            for iCPU = 1:numCPUs
                Threads.@spawn runJob(iCPU)
            end

            localResults::Vector{Tuple{T1,T1,T2}} = Vector{Tuple{T1,T1,T2}}()
            cpus = Set(iCPU for iCPU = 1:numCPUs)
            while length(cpus) > 0
                iCPU = rand(cpus)
                if jobFlags[iCPU] == false
                    yield()
                else
                    append!(localResults, jobOutputs[iCPU])
                    delete!(cpus, iCPU)
                end
            end
            sort!(localResults, by = x -> x[1])

            result = copy(initialResult)
            for block in localResults
                if length(result) == 0
                    append!(result, block[3])
                else
                    append!(result, blockPrefixFun(result[end], block[3]))
                end
            end
        end
        return result
    else
        if length(initialResult) == 0
            return mapFun(loopStart, loopEnd)
        else
            result = copy(initialResult)
            append!(result,
                blockPrefixFun(result[end], mapFun(loopStart, loopEnd)))
            return result
        end
    end
end

function mapReduce(
    loopStart::T1,
    loopEnd::T1,
    batchSize::T1,
    mapFun::Function,
    reduceFun::Function,
    x0::T2,
    parallel::Bool = true,
)::T2 where {T1<:Integer,T2<:Any}
    if parallel && Threads.threadid() == 1
        numCPUs = length(Sys.cpu_info())
        jobs = Channel{Tuple{T1,T1}}(numCPUs)
        jobOutputs = Vector{T2}(undef, numCPUs)
        jobFlags = fill(false, numCPUs)

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
            myResult = reduceFun(localResult)
            jobOutputs[iCPU] = myResult
            yield()
            jobFlags[iCPU] = true
        end # runJob

        bind(jobs, @async makeJobs())
        begin
            for iCPU = 1:numCPUs
                Threads.@spawn runJob(iCPU)
            end

            localResults = T2[x0]
            cpus = Set(iCPU for iCPU = 1:numCPUs)
            while length(cpus) > 0
                iCPU = rand(cpus)
                if jobFlags[iCPU] == false
                    yield()
                else
                    push!(localResults, jobOutputs[iCPU])
                    delete!(cpus, iCPU)
                end
            end
        end
        return reduceFun(localResults)
    else
        localResults = T2[x0]
        push!(localResults, mapFun(loopStart, loopEnd))
        return reduceFun(localResults)
    end
end

function mapOnly(
    loopStart::T1,
    loopEnd::T1,
    batchSize::T1,
    mapFun::Function,
    parallel::Bool = true,
) where {T1<:Integer}
    if parallel && Threads.threadid() == 1
        numCPUs = length(Sys.cpu_info())
        jobs = Channel{Tuple{T1,T1}}(numCPUs)
        jobFlags = fill(false, numCPUs)

        function makeJobs()
            for i::T1 = loopStart:batchSize:loopEnd
                put!(jobs, (i, min(i + batchSize - 1, loopEnd)))
            end
        end

        function runJob(iCPU::Int)
            for job in jobs
                mapFun(job[1], job[2])
            end # job
            jobFlags[iCPU] = true
        end # runJob

        bind(jobs, @async makeJobs())
        begin
            for iCPU = 1:numCPUs
                Threads.@spawn runJob(iCPU)
            end

            cpus = Set(iCPU for iCPU = 1:numCPUs)
            while length(cpus) > 0
                iCPU = rand(cpus)
                if jobFlags[iCPU] == false
                    yield()
                else
                    delete!(cpus, iCPU)
                end
            end
        end
    else
        mapFun(loopStart, loopEnd)
    end
end

end # module
