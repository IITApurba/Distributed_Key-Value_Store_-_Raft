#pragma once
#include <string>

namespace kvraft {

// Applied in strict log order by RaftNode once entries commit. Must be
// deterministic across replicas.
class StateMachine {
public:
    virtual ~StateMachine() = default;
    virtual std::string apply(const std::string& commandBytes, uint64_t logIndex) = 0;
    virtual std::string snapshot() const = 0;
    virtual void restore(const std::string& snapshotBytes) = 0;
};

} // namespace kvraft
