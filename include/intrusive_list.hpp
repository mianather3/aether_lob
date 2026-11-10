// AetherLOB - intrusive doubly linked list.
//
// The links live inside the node (T::prev / T::next), so insertion and removal
// are O(1), allocation-free, and need only the node pointer itself.
#pragma once

namespace aether {

template <class T>
class IntrusiveList {
public:
    [[nodiscard]] T* front() const noexcept { return head_; }
    [[nodiscard]] T* back() const noexcept { return tail_; }
    [[nodiscard]] bool empty() const noexcept { return head_ == nullptr; }

    void push_back(T* node) noexcept {
        node->prev = tail_;
        node->next = nullptr;
        if (tail_ != nullptr) {
            tail_->next = node;
        } else {
            head_ = node;
        }
        tail_ = node;
    }

    // Inserts `node` immediately before `pos`; pos == nullptr appends.
    void insert_before(T* pos, T* node) noexcept {
        if (pos == nullptr) {
            push_back(node);
            return;
        }
        node->prev = pos->prev;
        node->next = pos;
        if (pos->prev != nullptr) {
            pos->prev->next = node;
        } else {
            head_ = node;
        }
        pos->prev = node;
    }

    void erase(T* node) noexcept {
        if (node->prev != nullptr) {
            node->prev->next = node->next;
        } else {
            head_ = node->next;
        }
        if (node->next != nullptr) {
            node->next->prev = node->prev;
        } else {
            tail_ = node->prev;
        }
        node->prev = nullptr;
        node->next = nullptr;
    }

private:
    T* head_{nullptr};
    T* tail_{nullptr};
};

}  // namespace aether
