pub enum QueueKeys {
    Wait,
    Active,
    Prioritized,
    Events,
    Stalled,
    Limiter,
    Delayed,
    Paused,
    Meta,
    Pc,
    Marker,
    Metrics,
}

impl QueueKeys {
    pub fn as_str(&self) -> &'static str {
        match self {
            QueueKeys::Wait => "wait",
            QueueKeys::Active => "active",
            QueueKeys::Prioritized => "prioritized",
            QueueKeys::Events => "events",
            QueueKeys::Stalled => "stalled",
            QueueKeys::Limiter => "limiter",
            QueueKeys::Delayed => "delayed",
            QueueKeys::Paused => "paused",
            QueueKeys::Meta => "meta",
            QueueKeys::Pc => "pc",
            QueueKeys::Marker => "marker",
            QueueKeys::Metrics => "metrics",
        }
    }

    pub fn to_string(&self) -> String {
        self.as_str().to_string()
    }
}

impl Into<String> for QueueKeys {
    fn into(self) -> String {
        self.to_string()
    }
}

impl Into<&'static str> for QueueKeys {
    fn into(self) -> &'static str {
        self.as_str()
    }
}

impl std::fmt::Display for QueueKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
