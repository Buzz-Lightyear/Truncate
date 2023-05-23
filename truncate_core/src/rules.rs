// TODO: Maximum consecutive swaps / stalemate rule

pub enum WinCondition {
    Destination, // TODO: Implement
    Elimination, // TODO: Implement
}

pub enum Visibility {
    Standard,
    FogOfWar,
}

pub enum Truncation {
    Root,
    Larger, // TODO: Implement
    None,
}

pub enum OvertimeRule {
    FreeWildcard { period: usize },                   // TODO: Implement
    RemoveTiles { period: usize, phase_time: usize }, // TODO: Implement
    Elimination,                                      // TODO: Implement
}

pub enum Timing {
    PerPlayer {
        time_allowance: usize,
        overtime_rule: OvertimeRule, // TODO: Implement
    },
    PerTurn {
        // TODO: Implement
        time_allowance: usize,
    },
    Periodic {
        // TODO: Implement
        turn_delay: usize,
    },
    None, // TODO: Implement
}

pub enum TileDistribution {
    Standard,
}

pub enum TileBagBehaviour {
    Standard, // TODO: Implement
    Infinite, // TODO: Implement
}

pub struct BattleRules {
    pub length_delta: isize,
}

pub enum Swapping {
    Contiguous(SwapPenalty),
    Universal(SwapPenalty),
    None,
}

pub struct SwapPenalty {
    pub swap_threshold: usize,
    pub penalties: Vec<usize>,
}

pub struct GameRules {
    pub win_condition: WinCondition,
    pub visibility: Visibility,
    pub truncation: Truncation,
    pub timing: Timing,
    pub hand_size: usize,
    pub tile_distribution: TileDistribution,
    pub tile_bag_behaviour: TileBagBehaviour,
    pub battle_rules: BattleRules,
    pub swapping: Swapping,
}

impl Default for GameRules {
    fn default() -> Self {
        Self {
            win_condition: WinCondition::Destination,
            visibility: Visibility::FogOfWar,
            truncation: Truncation::None,
            timing: Timing::PerPlayer {
                time_allowance: 600,
                overtime_rule: OvertimeRule::FreeWildcard { period: 60 },
            },
            hand_size: 7,
            tile_distribution: TileDistribution::Standard,
            tile_bag_behaviour: TileBagBehaviour::Standard,
            battle_rules: BattleRules { length_delta: 2 },
            swapping: Swapping::Contiguous(SwapPenalty {
                swap_threshold: 2,
                penalties: vec![5, 10, 30, 60, 120, 240],
            }),
        }
    }
}
