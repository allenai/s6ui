use dear_imgui_rs::{Context, FontConfig};
use std::path::Path;

const DEFAULT_UI_FONT_SIZE: f32 = 14.0;

#[derive(Clone, Copy)]
struct FontCandidate {
    family: &'static str,
    path: &'static str,
}

#[cfg(target_os = "macos")]
const SYSTEM_MONOSPACE_FONT_CANDIDATES: &[FontCandidate] = &[
    FontCandidate {
        family: "Menlo",
        path: "/System/Library/Fonts/Menlo.ttc",
    },
    FontCandidate {
        family: "SF Mono",
        path: "/System/Library/Fonts/SFNSMono.ttf",
    },
    FontCandidate {
        family: "Monaco",
        path: "/System/Library/Fonts/Monaco.ttf",
    },
    FontCandidate {
        family: "Courier",
        path: "/System/Library/Fonts/Courier.ttc",
    },
];

#[cfg(target_os = "windows")]
const SYSTEM_MONOSPACE_FONT_CANDIDATES: &[FontCandidate] = &[
    FontCandidate {
        family: "Consolas",
        path: "C:\\Windows\\Fonts\\consola.ttf",
    },
    FontCandidate {
        family: "Cascadia Mono",
        path: "C:\\Windows\\Fonts\\CascadiaMono.ttf",
    },
    FontCandidate {
        family: "Cascadia Code",
        path: "C:\\Windows\\Fonts\\CascadiaCode.ttf",
    },
    FontCandidate {
        family: "Lucida Console",
        path: "C:\\Windows\\Fonts\\lucon.ttf",
    },
    FontCandidate {
        family: "Courier New",
        path: "C:\\Windows\\Fonts\\cour.ttf",
    },
];

#[cfg(all(unix, not(target_os = "macos")))]
const SYSTEM_MONOSPACE_FONT_CANDIDATES: &[FontCandidate] = &[
    FontCandidate {
        family: "DejaVu Sans Mono",
        path: "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    },
    FontCandidate {
        family: "Liberation Mono",
        path: "/usr/share/fonts/truetype/liberation2/LiberationMono-Regular.ttf",
    },
    FontCandidate {
        family: "Ubuntu Mono",
        path: "/usr/share/fonts/truetype/ubuntu/UbuntuMono-R.ttf",
    },
    FontCandidate {
        family: "Noto Sans Mono",
        path: "/usr/share/fonts/truetype/noto/NotoSansMono-Regular.ttf",
    },
    FontCandidate {
        family: "Droid Sans Mono",
        path: "/usr/share/fonts/truetype/droid/DroidSansMono.ttf",
    },
    FontCandidate {
        family: "DejaVu Sans Mono",
        path: "/usr/share/fonts/dejavu/DejaVuSansMono.ttf",
    },
    FontCandidate {
        family: "DejaVu Sans Mono",
        path: "/usr/share/fonts/TTF/DejaVuSansMono.ttf",
    },
    FontCandidate {
        family: "Liberation Mono",
        path: "/usr/share/fonts/liberation/LiberationMono-Regular.ttf",
    },
];

#[cfg(not(any(target_os = "macos", target_os = "windows", unix)))]
const SYSTEM_MONOSPACE_FONT_CANDIDATES: &[FontCandidate] = &[];

pub fn configure_imgui_fonts(context: &mut Context, verbose_logging: bool) {
    let mut fonts = context.fonts();
    fonts.clear();

    let debug_name = format!("s6ui UI {:.1}px", DEFAULT_UI_FONT_SIZE);
    let font_cfg = FontConfig::new()
        .name(&debug_name)
        .oversample_h(2)
        .oversample_v(2);

    for candidate in SYSTEM_MONOSPACE_FONT_CANDIDATES {
        if !Path::new(candidate.path).is_file() {
            continue;
        }

        if fonts
            .add_font_from_file_ttf(candidate.path, DEFAULT_UI_FONT_SIZE, Some(&font_cfg), None)
            .is_some()
        {
            if verbose_logging {
                eprintln!(
                    "Using system UI font: {} ({})",
                    candidate.family, candidate.path
                );
            }
            return;
        }
    }

    fonts.add_font_default(Some(&font_cfg));
    eprintln!(
        "Failed to locate a preferred system monospace font; falling back to Dear ImGui default"
    );
}
