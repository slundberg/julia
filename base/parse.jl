# This file is a part of Julia. License is MIT: http://julialang.org/license

## string to integer functions ##

function parse{T<:Integer}(::Type{T}, c::Char, base::Integer=36)
    a::Int = (base <= 36 ? 10 : 36)
    2 <= base <= 62 || throw(ArgumentError("invalid base: base must be 2 ≤ base ≤ 62, got $base"))
    d = '0' <= c <= '9' ? c-'0'    :
        'A' <= c <= 'Z' ? c-'A'+10 :
        'a' <= c <= 'z' ? c-'a'+a  : throw(ArgumentError("invalid digit: $(repr(c))"))
    d < base || throw(ArgumentError("invalid base $base digit $(repr(c))"))
    convert(T, d)
end

function parseint_next(s::AbstractString, startpos::Int, endpos::Int)
    (0 < startpos <= endpos) || (return Char(0), 0, 0)
    j = startpos
    c, startpos = next(s,startpos)
    c, startpos, j
end

function parseint_preamble(signed::Bool, base::Int, s::AbstractString, startpos::Int, endpos::Int)
    c, i, j = parseint_next(s, startpos, endpos)

    while isspace(c)
        c, i, j = parseint_next(s,i,endpos)
    end
    (j == 0) && (return 0, 0, 0)

    sgn = 1
    if signed
        if c == '-' || c == '+'
            (c == '-') && (sgn = -1)
            c, i, j = parseint_next(s,i,endpos)
        end
    end

    while isspace(c)
        c, i, j = parseint_next(s,i,endpos)
    end
    (j == 0) && (return 0, 0, 0)

    if base == 0
        if c == '0' && !done(s,i)
            c, i = next(s,i)
            base = c=='b' ? 2 : c=='o' ? 8 : c=='x' ? 16 : 10
            if base != 10
                c, i, j = parseint_next(s,i,endpos)
            end
        else
            base = 10
        end
    end
    return sgn, base, j
end

function tryparse_internal{S<:ByteString}(::Type{Bool}, sbuff::S, startpos::Int, endpos::Int, raise::Bool)
    len = endpos-startpos+1
    p = pointer(sbuff)+startpos-1
    (len == 4) && (0 == ccall(:memcmp, Int32, (Ptr{UInt8}, Ptr{UInt8}, UInt), p, "true", 4)) && (return Nullable(true))
    (len == 5) && (0 == ccall(:memcmp, Int32, (Ptr{UInt8}, Ptr{UInt8}, UInt), p, "false", 5)) && (return Nullable(false))
    raise && throw(ArgumentError("invalid Bool representation: $(repr(SubString(s,startpos,endpos)))"))
    Nullable{Bool}()
end


for I in [Int8,Int16,Int32,Int64,Int128,
          UInt8,UInt16,UInt32,UInt64,UInt128]

    s = I <: Signed ? 's' : 'u'
    b = sizeof(I) << 3
    r = b == 8 ? "[2 x i8]" : "{ i$b, i8 }" # LLVM return type

    for op in [:add,:sub,:mul]
        f = symbol(:safe_,op)
        @eval @inline function $f(x::$I, y::$I)
            x,n = Base.llvmcall((
            $"declare { i$b, i1 } @llvm.$(s)$(op).with.overflow.i$b(i$b, i$b)",
            $"""
            %xo = call { i$b, i1 } @llvm.$(s)$(op).with.overflow.i$b(i$b %0, i$b %1)
            %x  = extractvalue { i$b, i1 } %xo, 0
            %o1 = extractvalue { i$b, i1 } %xo, 1
            %o8 = zext i1 %o1 to i8
            %rx = insertvalue $r undef, i$b %x, 0
            %r  = insertvalue $r %rx, i8 %o8, 1
            ret $r %r"""),
            Tuple{$I,Bool},Tuple{$I,$I},x,y)
            Nullable(x,n)
        end
    end
end


function tryparse_internal{T<:Integer}(::Type{T}, s::AbstractString, startpos::Int, endpos::Int, base::Int, a::Int, raise::Bool)
    _n = Nullable{T}()
    sgn, base, i = parseint_preamble(T<:Signed, base, s, startpos, endpos)
    if i == 0
        raise && throw(ArgumentError("premature end of integer: $(repr(SubString(s,startpos,endpos)))"))
        return _n
    end
    c, i = parseint_next(s,i,endpos)
    if i == 0
        raise && throw(ArgumentError("premature end of integer: $(repr(SubString(s,startpos,endpos)))"))
        return _n
    end

    base = convert(T,base)
    m::T = div(typemax(T)-base+1,base)
    n::T = 0
    while n <= m
        d::T = '0' <= c <= '9' ? c-'0'    :
               'A' <= c <= 'Z' ? c-'A'+10 :
               'a' <= c <= 'z' ? c-'a'+a  : base
        if d >= base
            raise && throw(ArgumentError("invalid base $base digit $(repr(c)) in $(repr(SubString(s,startpos,endpos)))"))
            return _n
        end
        n *= base
        n += d
        if i > endpos
            n *= sgn
            return Nullable{T}(n)
        end
        c, i = next(s,i)
        isspace(c) && break
    end
    (T <: Signed) && (n *= sgn)
    while !isspace(c)
        d::T = '0' <= c <= '9' ? c-'0'    :
        'A' <= c <= 'Z' ? c-'A'+10 :
            'a' <= c <= 'z' ? c-'a'+a  : base
        if d >= base
            raise && throw(ArgumentError("invalid base $base digit $(repr(c)) in $(repr(SubString(s,startpos,endpos)))"))
            return _n
        end
        (T <: Signed) && (d *= sgn)

        safe_n = safe_mul(n, base)
        isnull(safe_n) || (safe_n = safe_add(get(safe_n), d))
        if isnull(safe_n)
            raise && throw(OverflowError())
            return _n
        end
        n = get(safe_n)
        (i > endpos) && return Nullable{T}(n)
        c, i = next(s,i)
    end
    while i <= endpos
        c, i = next(s,i)
        if !isspace(c)
            raise && throw(ArgumentError("extra characters after whitespace in $(repr(SubString(s,startpos,endpos)))"))
            return _n
        end
    end
    return Nullable{T}(n)
end
tryparse_internal{T<:Integer}(::Type{T}, s::AbstractString, base::Int, raise::Bool) =
    tryparse_internal(T,s,start(s),endof(s),base,raise)
tryparse_internal{T<:Integer}(::Type{T}, s::AbstractString, startpos::Int, endpos::Int, base::Int, raise::Bool) =
    tryparse_internal(T, s, startpos, endpos, base, base <= 36 ? 10 : 36, raise)
tryparse{T<:Integer}(::Type{T}, s::AbstractString, base::Int) =
    2 <= base <= 62 ? tryparse_internal(T,s,Int(base),false) : throw(ArgumentError("invalid base: base must be 2 ≤ base ≤ 62, got $base"))
tryparse{T<:Integer}(::Type{T}, s::AbstractString) = tryparse_internal(T,s,0,false)

function parse{T<:Integer}(::Type{T}, s::AbstractString, base::Integer)
    (2 <= base <= 62) || throw(ArgumentError("invalid base: base must be 2 ≤ base ≤ 62, got $base"))
    get(tryparse_internal(T, s, base, true))
end
parse{T<:Integer}(::Type{T}, s::AbstractString) = get(tryparse_internal(T, s, 0, true))

## stringifying integers more efficiently ##

string(x::Union{Int8,Int16,Int32,Int64,Int128}) = dec(x)

## string to float functions ##

tryparse(::Type{Float64}, s::ByteString) = ccall(:jl_try_substrtod, Nullable{Float64}, (Ptr{UInt8},Csize_t,Csize_t), s, 0, sizeof(s))
tryparse{T<:ByteString}(::Type{Float64}, s::SubString{T}) = ccall(:jl_try_substrtod, Nullable{Float64}, (Ptr{UInt8},Csize_t,Csize_t), s.string, s.offset, s.endof)

tryparse(::Type{Float32}, s::ByteString) = ccall(:jl_try_substrtof, Nullable{Float32}, (Ptr{UInt8},Csize_t,Csize_t), s, 0, sizeof(s))
tryparse{T<:ByteString}(::Type{Float32}, s::SubString{T}) = ccall(:jl_try_substrtof, Nullable{Float32}, (Ptr{UInt8},Csize_t,Csize_t), s.string, s.offset, s.endof)

tryparse{T<:Union{Float32,Float64}}(::Type{T}, s::AbstractString) = tryparse(T, bytestring(s))

function parse{T<:AbstractFloat}(::Type{T}, s::AbstractString)
    nf = tryparse(T, s)
    isnull(nf) ? throw(ArgumentError("invalid number format $(repr(s)) for $T")) : get(nf)
end

float(x::AbstractString) = parse(Float64,x)

float{S<:AbstractString}(a::AbstractArray{S}) = map!(float, similar(a,typeof(float(0))), a)

## interface to parser ##

function parse(str::AbstractString, pos::Int; greedy::Bool=true, raise::Bool=true)
    # pos is one based byte offset.
    # returns (expr, end_pos). expr is () in case of parse error.
    bstr = bytestring(str)
    ex, pos = ccall(:jl_parse_string, Any,
                    (Ptr{UInt8}, Csize_t, Int32, Int32),
                    bstr, sizeof(bstr), pos-1, greedy ? 1:0)
    if raise && isa(ex,Expr) && is(ex.head,:error)
        throw(ParseError(ex.args[1]))
    end
    if ex == ()
        raise && throw(ParseError("end of input"))
        ex = Expr(:error, "end of input")
    end
    ex, pos+1 # C is zero-based, Julia is 1-based
end

function parse(str::AbstractString; raise::Bool=true)
    ex, pos = parse(str, 1, greedy=true, raise=raise)
    if isa(ex,Expr) && ex.head === :error
        return ex
    end
    if !done(str, pos)
        raise && throw(ParseError("extra token after end of expression"))
        return Expr(:error, "extra token after end of expression")
    end
    return ex
end
