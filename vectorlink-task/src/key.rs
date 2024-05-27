pub static CLAIMS_PREFIX: &[u8] = b"/services/claims/";
pub static TASKS_PREFIX: &[u8] = b"/services/tasks/";
/// the first possible key after all tasks that is not a task
pub static QUEUE_PREFIX: &[u8] = b"/services/queue/";
pub static INTERRUPT_PREFIX: &[u8] = b"/services/interrupt/";
pub static WAIT_PREFIX: &[u8] = b"/services/waits/";

pub fn claim_key_task_id(claim_key: &[u8]) -> &[u8] {
    &claim_key[CLAIMS_PREFIX.len()..]
}
pub fn task_key_task_id(task_key: &[u8]) -> &[u8] {
    &task_key[TASKS_PREFIX.len()..]
}
pub fn queue_key_task_id(queue_key: &[u8]) -> &[u8] {
    &queue_key[QUEUE_PREFIX.len()..]
}
pub fn interrupt_key_task_id(interrupt_key: &[u8]) -> &[u8] {
    &interrupt_key[INTERRUPT_PREFIX.len()..]
}

pub fn wait_key_task_id(wait_key: &[u8]) -> &[u8] {
    &wait_key[WAIT_PREFIX.len()..]
}

pub fn concat_bytes(b1: &[u8], b2: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(b1.len() + b2.len());
    result.extend_from_slice(b1);
    result.extend_from_slice(b2);

    result
}

pub fn claim_key(task_id: &[u8]) -> Vec<u8> {
    concat_bytes(CLAIMS_PREFIX, task_id)
}

pub fn task_key(task_id: &[u8]) -> Vec<u8> {
    concat_bytes(TASKS_PREFIX, task_id)
}

pub fn queue_key(task_id: &[u8]) -> Vec<u8> {
    concat_bytes(QUEUE_PREFIX, task_id)
}

pub fn interrupt_key(task_id: &[u8]) -> Vec<u8> {
    concat_bytes(INTERRUPT_PREFIX, task_id)
}

pub fn wait_key(task_id: &[u8]) -> Vec<u8> {
    concat_bytes(WAIT_PREFIX, task_id)
}

/// calculates the first key that would not be part of a prefix
pub fn key_after_prefix(key: &[u8]) -> Vec<u8> {
    let mut key_bytes = key.to_vec();

    for b in key_bytes.iter_mut().rev() {
        if *b == 255 {
            // we need to wrap around
            *b = 0
        } else {
            // no wrapping around.
            *b += 1;
            return key_bytes;
        }
    }

    // if we are here, we wrapped around all the way and we just have
    // to push a 1 to the front.
    key_bytes.insert(0, 1);

    key_bytes
}

/// Calculates the next possible key by appending \x00.
pub fn get_increment_key(key: &[u8]) -> Vec<u8> {
    let mut key_bytes = Vec::with_capacity(key.len() + 1);
    key_bytes.extend_from_slice(key);
    key_bytes.push(0);

    key_bytes
}
