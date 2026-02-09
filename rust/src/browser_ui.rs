use crate::browser_model::BrowserModel;
use dear_imgui_rs::*;
pub fn render(ui: &Ui, model: &mut BrowserModel) {
    model.process_events();

    let display_size = ui.io().display_size();

    ui.window("##main")
        .position([0.0, 0.0], Condition::Always)
        .size(display_size, Condition::Always)
        .flags(
            WindowFlags::NO_TITLE_BAR
                | WindowFlags::NO_RESIZE
                | WindowFlags::NO_MOVE
                | WindowFlags::NO_COLLAPSE,
        )
        .build(|| {
            render_toolbar(ui, model);
            ui.separator();
            render_path_bar(ui, model);
            ui.separator();
            render_content(ui, model);
            render_status_bar(ui, model);
        });
}

fn render_toolbar(ui: &Ui, model: &mut BrowserModel) {
    ui.text("Profile:");
    ui.same_line();
    ui.set_next_item_width(200.0);

    let profiles = model.profiles.clone();
    let mut profile_idx = model.selected_profile_idx;
    if ui.combo_simple_string("##profile", &mut profile_idx, &profiles) {
        model.select_profile(profile_idx);
    }

    ui.same_line();
    ui.text("Bucket:");
    ui.same_line();
    ui.set_next_item_width(250.0);

    let buckets = model.buckets.clone();
    let mut bucket_idx = model.selected_bucket_idx;
    if ui.combo_simple_string("##bucket", &mut bucket_idx, &buckets) {
        model.select_bucket(bucket_idx);
    }
}

fn render_path_bar(ui: &Ui, model: &mut BrowserModel) {
    ui.text("Path:");
    ui.same_line();

    let prefix = model.current_prefix.clone();
    let mut nav_target: Option<String> = None;

    if ui.small_button("/") {
        nav_target = Some(String::new());
    }

    if !prefix.is_empty() {
        let parts: Vec<&str> = prefix.trim_end_matches('/').split('/').collect();
        let mut accumulated = String::new();
        for (i, part) in parts.iter().enumerate() {
            accumulated.push_str(part);
            accumulated.push('/');
            ui.same_line();
            ui.text(">");
            ui.same_line();
            let _id = ui.push_id(i as i32);
            if ui.small_button(part) {
                nav_target = Some(accumulated.clone());
            }
        }
    }

    if let Some(target) = nav_target {
        model.navigate_to_prefix(target);
    }
}

fn render_content(ui: &Ui, model: &mut BrowserModel) {
    let avail = ui.content_region_avail();
    let status_bar_height = 25.0;
    let content_height = avail[1] - status_bar_height;
    let left_width = avail[0] * 0.5;

    ui.child_window("##file_list")
        .size([left_width, content_height])
        .flags(WindowFlags::HORIZONTAL_SCROLLBAR)
        .build(&ui, || {
            render_file_list(ui, model);
        });

    ui.same_line();

    ui.child_window("##preview")
        .size([0.0, content_height])
        .build(&ui, || {
            render_preview(ui, model);
        });
}

fn render_file_list(ui: &Ui, model: &mut BrowserModel) {
    let folder_count = model.folders.len();
    let obj_count = model.objects.len();
    let prefix = model.current_prefix.clone();

    if folder_count + obj_count == 0 {
        if model.loading {
            ui.text("Loading...");
        } else if model.buckets.is_empty() {
            ui.text("Select a profile to see buckets");
        } else {
            ui.text("(empty)");
        }
        return;
    }

    let mut go_up = false;
    let mut enter_folder: Option<usize> = None;
    let mut new_selection: Option<usize> = None;

    // Back button
    if !prefix.is_empty() {
        if ui.selectable("..") {
            go_up = true;
        }
    }

    // Items with clipper for large lists
    let total = (folder_count + obj_count) as i32;
    let clipper = ListClipper::new(total).begin(ui);
    for display_idx in clipper.iter() {
        let idx = display_idx as usize;
        let _id = ui.push_id(display_idx);

        let is_selected = model.selected_item == Some(idx);

        if idx < folder_count {
            let label = format!("{}/", &model.folders[idx]);
            if ui
                .selectable_config(&label)
                .selected(is_selected)
                .build()
            {
                enter_folder = Some(idx);
            }
        } else {
            let obj_idx = idx - folder_count;
            let obj = &model.objects[obj_idx];
            let display_name = obj.key.strip_prefix(&prefix).unwrap_or(&obj.key);
            let label = format!("{}    {}", display_name, format_size(obj.size));
            if ui
                .selectable_config(&label)
                .selected(is_selected)
                .build()
            {
                new_selection = Some(idx);
            }
        }
    }

    // Apply deferred actions
    if go_up {
        model.navigate_up();
    } else if let Some(folder_idx) = enter_folder {
        model.enter_folder(folder_idx);
    } else if let Some(sel) = new_selection {
        model.selected_item = Some(sel);
    }
}

fn render_preview(ui: &Ui, model: &BrowserModel) {
    let selected = match model.selected_item {
        Some(idx) => idx,
        None => {
            ui.text("Select a file to preview");
            return;
        }
    };

    let folders = &model.folders;
    if selected < folders.len() {
        ui.text(format!("Folder: {}/", folders[selected]));
        return;
    }

    let obj_idx = selected - folders.len();
    if obj_idx < model.objects.len() {
        let obj = &model.objects[obj_idx];
        let display_name = obj
            .key
            .strip_prefix(&model.current_prefix)
            .unwrap_or(&obj.key);
        ui.text(format!("File: {}", display_name));
        ui.separator();
        ui.text(format!("Full key: {}", obj.key));
        ui.text(format!("Size: {}", format_size(obj.size)));
        ui.text(format!("Last modified: {}", obj.last_modified));
    }
}

fn render_status_bar(ui: &Ui, model: &BrowserModel) {
    ui.separator();
    if let Some(err) = &model.error_message {
        ui.text_colored([1.0, 0.3, 0.3, 1.0], format!("Error: {}", err));
    } else if model.loading {
        let count = model.folders.len() + model.objects.len();
        if count > 0 {
            ui.text(format!("Loading... ({} items so far)", count));
        } else {
            ui.text("Loading...");
        }
    } else {
        let folders = model.folders.len();
        let objects = model.objects.len();
        ui.text(format!("{} folders, {} files", folders, objects));
    }
}

fn format_size(bytes: i64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;
    const TB: f64 = 1024.0 * 1024.0 * 1024.0 * 1024.0;

    let b = bytes as f64;
    if b >= TB {
        format!("{:.1} TB", b / TB)
    } else if b >= GB {
        format!("{:.1} GB", b / GB)
    } else if b >= MB {
        format!("{:.1} MB", b / MB)
    } else if b >= KB {
        format!("{:.1} KB", b / KB)
    } else {
        format!("{} B", bytes)
    }
}
