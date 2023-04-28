use eframe::{
    egui::{self, Layout},
    emath::Align,
};
use epaint::Rect;

use crate::theming::Theme;

pub struct HandSquareUI {
    empty: bool,
}

impl HandSquareUI {
    pub fn new() -> Self {
        Self { empty: false }
    }

    pub fn empty(mut self, empty: bool) -> Self {
        self.empty = empty;
        self
    }

    pub fn render(
        &self,
        ui: &mut egui::Ui,
        theme: &Theme,
        contents: impl FnOnce(&mut egui::Ui, &Theme),
    ) -> (egui::Response, Rect) {
        let (rect, response) = ui.allocate_exact_size(
            egui::vec2(theme.grid_size, theme.grid_size),
            egui::Sense::hover(),
        );
        let interact_rect = rect.shrink(theme.tile_margin);
        let response = ui.interact(
            interact_rect,
            response.id.with("interact"),
            egui::Sense::click(),
        );

        if ui.is_rect_visible(rect) {
            let is_hovered = ui.rect_contains_pointer(interact_rect);

            let show_contents = !self.empty || !is_hovered;

            if show_contents {
                contents(
                    &mut ui.child_ui(rect, Layout::left_to_right(Align::TOP)),
                    theme,
                );
            }

            if is_hovered {
                if self.empty && !ui.ctx().memory(|mem| mem.is_anything_being_dragged()) {
                    ui.painter().rect_filled(
                        rect.shrink(theme.tile_margin),
                        theme.rounding,
                        theme.outlines,
                    );
                }
            }
        }

        (response, rect)
    }
}
