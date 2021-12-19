using Test
include("../src/WJFParallelTask.jl")

@testset "map prefix" begin
    a = Float64[i for i = 1:1000000]
    @everywhere function mapFun(i1, i2, segment::Vector{Float64})
        result = copy(segment)
        for i = 1:1000000
            rand()
        end
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
    @everywhere function mapFun2(i1, i2, segment::Vector{Float64})
        return sum(segment)
    end
    function reduceFun(xs::Vector{Float64})
        return sum(xs)
    end
    b = mapPrefix(
        1,length(a),10, mapFun, blockPrefixFun, a)
    c = mapReduce(
        1,length(a),10, mapFun2, reduceFun, a, 0.0
    )
    aResult = Vector{Float64}(undef, length(a))
    aResult[1] = a[1]
    for i = 2:length(a)
        aResult[i] = aResult[i-1] + a[i]
    end
    err = sum(abs.(aResult .- b))
    println("err = $err")
    @test err ≈ 0
    @test c ≈ aResult[end]
end
