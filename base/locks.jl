# This file is a part of Julia. License is MIT: http://julialang.org/license

import Base: _uv_hook_close, wait, isopen, close, unsafe_convert

export SpinLock, Mutex, AsyncCondition,
    init_lock!, destroy_lock!, lock!, trylock!, unlock!


##########################################
# Atomic Locks
##########################################

abstract AbstractLock

# Test-and-test-and-set spin locks are quickest up to about 30ish
# contending threads. If you have more contention than that, perhaps
# a lock is the wrong way to synchronize.
immutable TatasLock <: AbstractLock
    handle::Atomic{Int}
    TatasLock() = new(Atomic{Int}(0))
end

typealias SpinLock TatasLock

function lock!(l::TatasLock)
    while true
        if l.handle[] == 0
            p = atomic_xchg!(l.handle, 1)
            if p == 0
                return 0
            end
        end
        ccall(:jl_cpu_pause, Void, ())
        # Temporary solution before we have gc transition support in codegen.
        # This could mess up gc state when we add codegen support.
        # Use these as a safe point
        gc_state = ccall(:jl_gc_safe_enter, Int8, ())
        ccall(:jl_gc_safe_leave, Void, (Int8,), gc_state)
    end
end

function trylock!(l::TatasLock)
    if l.handle[] == 0
        return atomic_xchg!(l.handle, 1)
    end
    return 1
end

function unlock!(l::TatasLock)
    l.handle[] = 0
    ccall(:jl_cpu_wake, Void, ())
    return 0
end


# Recursive test-and-test-and-set lock. Slower.
immutable RecursiveTatasLock <: AbstractLock
    ownertid::Atomic{Int16}
    handle::Atomic{Int}
    RecursiveTatasLock() = new(Atomic{Int16}(0), Atomic{Int}(0))
end

typealias RecursiveSpinLock RecursiveTatasLock

function lock!(l::RecursiveTatasLock)
    if l.ownertid[] == threadid()
        l.handle[] += 1
        return 0
    end
    while true
        if l.handle[] == 0
            if atomic_cas!(l.handle, 0, 1)
                l.ownertid[] = threadid()
                return 0
            end
        end
        ccall(:jl_cpu_pause, Void, ())
        # Temporary solution before we have gc transition support in codegen.
        # This could mess up gc state when we add codegen support.
        # Use these as a safe point
        gc_state = ccall(:jl_gc_safe_enter, Int8, ())
        ccall(:jl_gc_safe_leave, Void, (Int8,), gc_state)
    end
end

function trylock!(l::RecursiveTatasLock)
    if l.ownertid[] == threadid()
        l.handle[] += 1
        return 0
    end
    if l.handle[] == 0
        if atomic_cas!(l.handle, 0, 1)
            l.ownertid[] = threadid()
            return 0
        end
        return 1
    end
    return 1
end

function unlock!(l::RecursiveTatasLock)
    if l.ownertid[] != threadid()
        return 1
    end
    if l.handle[] == 1
        l.ownertid[] = 0
        l.handle[] = 0
        ccall(:jl_cpu_wake, Void, ())
    else
        l.handle[] -= 1
    end
    return 0
end


##########################################
# System Mutexes
##########################################

# These are mutexes from libuv, which abstract pthread mutexes and
# Windows critical sections. We're doing some error checking (and
# paying for it in overhead), but regardless, in some situations,
# passing a bad parameter will cause an abort.

# TODO: how defensive to get, and how to turn it off?
# TODO: how to catch an abort?

const UV_MUTEX_SIZE = ccall(:jl_sizeof_uv_mutex, Cint, ())

type Mutex <: AbstractLock
    ownertid::Int16
    handle::Ptr{Void}
    function Mutex()
        m = new(zero(Int16), Libc.malloc(UV_MUTEX_SIZE))
        ccall(:uv_mutex_init, Void, (Ptr{Void},), m.handle)
        finalizer(m, _uv_hook_close)
        return m
    end
end

unsafe_convert(::Type{Ptr{Void}}, m::Mutex) = m.handle

function _uv_hook_close(x::Mutex)
    h = x.handle
    x.handle = C_NULL
    ccall(:uv_mutex_destroy, Void, (Ptr{Void},), h)
    Libc.free(h)
    nothing
end

function lock!(m::Mutex)
    if m.ownertid == threadid()
        return 0
    end
    # Temporary solution before we have gc transition support in codegen.
    # This could mess up gc state when we add codegen support.
    gc_state = ccall(:jl_gc_safe_enter, Int8, ())
    ccall(:uv_mutex_lock, Void, (Ptr{Void},), m)
    ccall(:jl_gc_safe_leave, Void, (Int8,), gc_state)
    m.ownertid = threadid()
    return 0
end

function trylock!(m::Mutex)
    if m.ownertid == threadid()
        return 0
    end
    r = ccall(:uv_mutex_trylock, Cint, (Ptr{Void},), m)
    if r == 0
        m.ownertid = threadid()
    end
    return r
end

function unlock!(m::Mutex)
    if m.ownertid != threadid()
        return Base.UV_EPERM
    end
    m.ownertid = 0
    ccall(:uv_mutex_unlock, Void, (Ptr{Void},), m)
    return 0
end



##########################################
# Async Notifications
##########################################

"""
    AsyncCondition()

Create a async condition that wakes up tasks waiting for it (by calling `wait` on the object)
when notified from C by a call to uv_async_send.
Waiting tasks are woken with an error when the object is closed (by `close`).
Use `isopen` to check whether it is still active.
"""
type AsyncCondition
    handle::Ptr{Void}
    cond::Condition

    function AsyncCondition()
        this = new(Libc.malloc(Base._sizeof_uv_async), Condition())
        Base.associate_julia_struct(this.handle, this)
        Base.preserve_handle(this)
        err = ccall(:uv_async_init, Cint, (Ptr{Void}, Ptr{Void}, Ptr{Void}),
            Base.eventloop(), this, uv_jl_asynccb::Ptr{Void})
        this
    end
end

unsafe_convert(::Type{Ptr{Void}}, async::AsyncCondition) = async.handle

function wait(async::AsyncCondition)
    isopen(async) || throw(EOFError())
    wait(async.cond)
end

isopen(t::AsyncCondition) = (t.handle != C_NULL)

close(t::AsyncCondition) = ccall(:jl_close_uv, Void, (Ptr{Void},), t)

function _uv_hook_close(async::AsyncCondition)
    async.handle = C_NULL
    Base.unpreserve_handle(async)
    Base.notify_error(async.cond, EOFError())
    nothing
end

function uv_asynccb(handle::Ptr{Void})
    async = Base.@handle_as handle AsyncCondition
    notify(async.cond)
    nothing
end

"""
    AsyncCondition(callback::Function)

Create a async condition that calls the given `callback` function. The `callback` is passed one argument,
the async condition object itself.
"""
function AsyncCondition(cb::Function)
    async = AsyncCondition()
    waiter = @task begin
        while isopen(async)
            success = try
                wait(async)
                true
            catch # ignore possible exception on close()
                false
            end
            success && cb(async)
        end
    end
    # must start the task right away so that it can wait for the AsyncCondition before
    # we re-enter the event loop. this avoids a race condition. see issue #12719
    Base.enq_work(current_task())
    yieldto(waiter)
    return async
end

##########################################
# Timer
##########################################

"""
    Timer(delay, repeat=0)

Create a timer that wakes up tasks waiting for it (by calling `wait` on the timer object) at
a specified interval.  Times are in seconds.  Waiting tasks are woken with an error when the
timer is closed (by `close`). Use `isopen` to check whether a timer is still active.
"""
type Timer
    handle::Ptr{Void}
    cond::Condition
    isopen::Bool

    function Timer(timeout::Real, repeat::Real=0.0)
        timeout ≥ 0 || throw(ArgumentError("timer cannot have negative timeout of $timeout seconds"))
        repeat ≥ 0 || throw(ArgumentError("timer cannot have negative repeat interval of $repeat seconds"))

        this = new(Libc.malloc(Base._sizeof_uv_timer), Condition(), true)
        err = ccall(:uv_timer_init, Cint, (Ptr{Void}, Ptr{Void}), Base.eventloop(), this)
        if err != 0
            #TODO: this codepath is currently not tested
            Libc.free(this.handle)
            this.handle = C_NULL
            throw(UVError("uv_make_timer",err))
        end

        Base.associate_julia_struct(this.handle, this)
        Base.preserve_handle(this)

        ccall(:uv_update_time, Void, (Ptr{Void},), Base.eventloop())
        ccall(:uv_timer_start,  Cint,  (Ptr{Void}, Ptr{Void}, UInt64, UInt64),
              this, uv_jl_timercb::Ptr{Void},
              UInt64(round(timeout * 1000)) + 1, UInt64(round(repeat * 1000)))
        return this
    end
end

unsafe_convert(::Type{Ptr{Void}}, t::Timer) = t.handle

function wait(t::Timer)
    isopen(t) || throw(EOFError())
    wait(t.cond)
end

isopen(t::Timer) = t.isopen

function close(t::Timer)
    if t.handle != C_NULL
        t.isopen = false
        ccall(:uv_timer_stop, Cint, (Ptr{Void},), t)
        ccall(:jl_close_uv, Void, (Ptr{Void},), t)
    end
    nothing
end

function _uv_hook_close(t::Timer)
    Base.unpreserve_handle(t)
    Base.disassociate_julia_struct(t)
    t.handle = C_NULL
    t.isopen = false
    Base.notify_error(t.cond, EOFError())
    nothing
end

function uv_timercb(handle::Ptr{Void})
    t = Base.@handle_as handle Timer
    if ccall(:uv_timer_get_repeat, UInt64, (Ptr{Void},), t) == 0
        # timer is stopped now
        close(t)
    end
    notify(t.cond)
    nothing
end

"""
    sleep(seconds)

Block the current task for a specified number of seconds. The minimum sleep time is 1
millisecond or input of `0.001`.
"""
function sleep(sec::Real)
    sec ≥ 0 || throw(ArgumentError("cannot sleep for $sec seconds"))
    wait(Timer(sec))
    nothing
end

# timer with repeated callback
"""
    Timer(callback::Function, delay, repeat=0)

Create a timer to call the given `callback` function. The `callback` is passed one argument,
the timer object itself. The callback will be invoked after the specified initial `delay`,
and then repeating with the given `repeat` interval. If `repeat` is `0`, the timer is only
triggered once. Times are in seconds. A timer is stopped and has its resources freed by
calling `close` on it.
"""
function Timer(cb::Function, timeout::Real, repeat::Real=0.0)
    t = Timer(timeout, repeat)
    waiter = @task begin
        while isopen(t)
            success = try
                wait(t)
                true
            catch # ignore possible exception on close()
                false
            end
            success && cb(t)
        end
    end
    # must start the task right away so that it can wait for the Timer before
    # we re-enter the event loop. this avoids a race condition. see issue #12719
    Base.enq_work(current_task())
    yieldto(waiter)
    return t
end

function __init__()
    global uv_jl_asynccb       = cfunction(uv_asynccb, Void, (Ptr{Void},))
    global uv_jl_timercb       = cfunction(uv_timercb, Void, (Ptr{Void},))
end
