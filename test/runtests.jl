using Test
using WJFParallelTask

@testset "map prefix" begin
    a = Float64[i for i = 1:4000000]
    @everywhere function mapFun(i1, i2, segment::Vector{Float64}, globalStates::Dict{String,Any})
        result = copy(segment)
        for j = 2:length(result)
            result[j] += result[j - 1]
        end
        return result
    end
    @everywhere function mapFun1(i1, i2, data::Vector{Float64}, globalStates::Dict{String,Any})
        result = copy(data[i1:i2])
        for j = 2:length(result)
            result[j] += result[j - 1]
        end
        return result
    end
    @everywhere function blockPrefixFun(x0::Float64, xs::Vector{Float64})
        result = Vector{Float64}(undef, length(xs))
        for i = 1:length(xs)
            result[i] = xs[i] + x0
        end
        return result
    end
    @everywhere function mapFun2(i1, i2, segment::Vector{Float64}, globalStates::Dict{String,Any})
        return sum(segment)
    end
    @everywhere function mapFun3(i1, i2, data::Vector{Float64}, globalStates::Dict{String,Any})
        return sum(data[i1:i2])
    end
    function reduceFun(xs::Vector{Float64})
        return sum(xs)
    end
    aResult = Vector{Float64}(undef, length(a))
    aResult[1] = a[1]
    for i = 2:length(a)
        aResult[i] = aResult[i-1] + a[i]
    end
    @time b = mapPrefix(a, 1000, mapFun, blockPrefixFun, 0.0)
    err = sum(abs.(aResult .- b))
    println("err = $err")
    @time b1 = mapPrefix(a, 1000, mapFun1, blockPrefixFun, 0.0, copyData=true)
    err1 = sum(abs.(aResult .- b1))
    println("err1 = $err1")
    @time c = mapReduce(a, 1000, mapFun2, reduceFun, 0.0)
    @time c1 = mapReduce(a, 1000, mapFun3, reduceFun, 0.0, copyData=true)
    @test err ≈ 0
    @test err1 ≈ 0
    @test c ≈ aResult[end]
    @test c1 ≈ aResult[end]
end
