use crate::frame::Reason;
use crate::proto::{MAX_WINDOW_SIZE, WindowSize};

const UNCLAIMED_NUMERATOR: i32 = 1;
const UNCLAIMED_DENOMINATOR: i32 = 2;

#[derive(Copy, Clone, Debug)]
pub struct FlowControl {
    /// RECV: What peer knows they can send
    /// SEND: What peer allows us to send (same as our capacity)
    window: Window,

    /// RECV: Our actual capacity (stays at initial for immediate consumption)
    /// SEND: Not used (same as window)
    capacity: Window,
}

impl FlowControl {
    pub fn new(init_window_sz: u32) -> FlowControl {
        FlowControl {
            window: Window(init_window_sz as i32),
            capacity: Window(init_window_sz as i32),
        }
    }

    pub fn window_size(&self) -> WindowSize {
        self.window.as_size()
    }

    pub fn available(&self) -> WindowSize {
        self.window.as_size()
    }

    pub fn dec_window(&mut self, sz: WindowSize) -> Result<(), Reason> {
        self.window.decrease_by(sz)
    }

    pub fn inc_window(&mut self, sz: WindowSize) -> Result<(), Reason> {
        self.window.increase_by(sz)
    }

    pub fn hold_capacity(&mut self, size: WindowSize) {
        self.capacity
            .decrease_by(size)
            .expect("receive capacity underflow");
    }

    pub fn release_capacity(&mut self, size: WindowSize) {
        self.capacity
            .increase_by(size)
            .expect("receive capacity overflow");
    }

    /// Check if should send WINDOW_UPDATE
    pub fn should_send_window_update(&self) -> Option<WindowSize> {
        if self.window >= self.capacity {
            return None;
        }

        let unclaimed = self.capacity.0 - self.window.0;
        let threshold =
            self.capacity.0 / UNCLAIMED_DENOMINATOR * UNCLAIMED_NUMERATOR;

        if unclaimed < threshold {
            None
        } else {
            Some(unclaimed as WindowSize)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd)]
pub struct Window(i32);

impl Window {
    pub fn as_size(&self) -> WindowSize {
        if self.0 < 0 {
            0
        } else {
            self.0 as WindowSize
        }
    }

    pub fn decrease_by(&mut self, sz: WindowSize) -> Result<(), Reason> {
        let Some(v) = self.0.checked_sub(sz as i32) else {
            return Err(Reason::FLOW_CONTROL_ERROR);
        };
        self.0 = v;
        Ok(())
    }

    pub fn increase_by(&mut self, sz: WindowSize) -> Result<(), Reason> {
        let (val, overflow) = self.0.overflowing_add(sz as i32);
        if overflow || val > MAX_WINDOW_SIZE as i32 {
            return Err(Reason::FLOW_CONTROL_ERROR);
        }
        self.0 = val;
        Ok(())
    }
}
