use eframe::egui::{self, Id};
use epaint::{Color32, TextShape, Vec2};
use std::f32;

use crate::{utils::glyph_utils::Glypher, utils::*};

pub enum CharacterOrient {
    North,
    East,
    South,
    West,
}

pub struct CharacterUI {
    letter: char,
    orientation: CharacterOrient,
    hovered: bool,
    active: bool,
    selected: bool,
    ghost: bool,
    truncated: bool,
    defeated: bool,
    gone: bool,
}

impl CharacterUI {
    pub fn new(letter: char, orientation: CharacterOrient) -> Self {
        Self {
            letter,
            orientation,
            hovered: false,
            active: true,
            selected: false,
            ghost: false,
            truncated: false,
            defeated: false,
            gone: false,
        }
    }

    pub fn hovered(mut self, hovered: bool) -> Self {
        self.hovered = hovered;
        self
    }

    pub fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    pub fn active(mut self, active: bool) -> Self {
        self.active = active;
        self
    }

    pub fn ghost(mut self, ghost: bool) -> Self {
        self.ghost = ghost;
        self
    }

    pub fn truncated(mut self, truncated: bool) -> Self {
        self.truncated = truncated;
        self
    }

    pub fn defeated(mut self, defeated: bool) -> Self {
        self.defeated = defeated;
        self
    }

    pub fn gone(mut self, gone: bool) -> Self {
        self.gone = gone;
        self
    }
}

impl CharacterUI {
    fn char_color(&self, theme: &Theme) -> Color32 {
        if self.gone {
            theme.grass.darken().gamma_multiply(0.75)
        } else if self.ghost {
            theme.outlines
        } else if !self.active {
            theme.outlines
        } else if self.hovered || self.selected {
            theme.text.darken()
        } else if self.defeated {
            theme.text.gamma_multiply(0.75)
        } else if self.truncated {
            theme.text.gamma_multiply(0.75)
        } else {
            theme.text
        }
    }

    pub fn render(self, ui: &mut egui::Ui, rect: egui::Rect, theme: &Theme) {
        let color = self.char_color(theme);
        self.render_with_color(ui, rect, theme, color);
    }

    pub fn render_with_color(
        self,
        ui: &mut egui::Ui,
        mut rect: egui::Rect,
        theme: &Theme,
        color: Color32,
    ) {
        if self.gone {
            match self.orientation {
                CharacterOrient::North => rect.set_top(rect.top() + rect.height() * 0.2),
                CharacterOrient::South => rect.set_bottom(rect.bottom() - rect.height() * 0.2),
                CharacterOrient::West | CharacterOrient::East => unreachable!(),
            }
        }

        let galley = ui.painter().layout_no_wrap(
            self.letter.to_string(),
            egui::FontId::new(
                theme.letter_size,
                egui::FontFamily::Name("Truncate-Heavy".into()),
            ),
            color,
        );

        let charshift: Vec2 = ui.memory_mut(|mem| {
            if let Some(measurement) = mem.data.get_temp(Id::from(self.letter.to_string())) {
                return measurement;
            }
            let glyph_measure: Glypher = mem.data.get_temp(Id::NULL).unwrap();
            let measurement = glyph_measure.measure(self.letter);
            mem.data
                .insert_temp(Id::from(self.letter.to_string()), measurement);
            measurement
        });

        let (angle, shift) = match self.orientation {
            CharacterOrient::North => (
                0.0,
                egui::Vec2::new(
                    (rect.width() - galley.size().x) * 0.5 + charshift.x * theme.letter_size,
                    (rect.height() - galley.mesh_bounds.height()) * 0.5,
                ),
            ),
            CharacterOrient::East => unimplemented!("Render Sideways characters"),
            CharacterOrient::South => (
                f32::consts::PI,
                egui::Vec2::new(
                    galley.size().x + (rect.width() - galley.size().x) * 0.5
                        - charshift.x * theme.letter_size,
                    // 0.4 centers the tile better when upside down, though logically it should be 0.5
                    // (our centering and egui's centering are scuffling a little bit here)
                    galley.mesh_bounds.height()
                        + (rect.height() - galley.mesh_bounds.height()) * 0.4,
                ),
            ),
            CharacterOrient::West => unimplemented!("Render Sideways characters"),
        };

        ui.painter().add(TextShape {
            angle,
            override_text_color: Some(color),
            ..TextShape::new(rect.left_top() + shift, galley)
        });
    }
}
