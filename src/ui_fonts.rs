use dear_imgui_rs::{Context, FontAtlas, FontConfig, sys};
use std::path::Path;

const DEFAULT_UI_FONT_SIZE: f32 = 14.0;

#[derive(Clone, Copy)]
struct FontCandidate {
    family: &'static str,
    path: &'static str,
}

const HAN_AND_KANA_GLYPH_RANGES: &[sys::ImWchar] = &[
    0x2000, 0x206F, // General punctuation
    0x3000, 0x30FF, // CJK punctuation, Hiragana, Katakana
    0x31F0, 0x31FF, // Katakana Phonetic Extensions
    0x3400, 0x4DBF, // CJK Extension A
    0x4E00, 0x9FFF, // CJK Unified Ideographs
    0xF900, 0xFAFF, // CJK Compatibility Ideographs
    0xFF00, 0xFFEF, // Full-width / half-width forms
    0,
];

const HANGUL_GLYPH_RANGES: &[sys::ImWchar] = &[
    0x2000, 0x206F, // General punctuation
    0x3000, 0x303F, // CJK punctuation
    0x1100, 0x11FF, // Hangul Jamo
    0x3131, 0x318E, // Hangul Compatibility Jamo
    0xAC00, 0xD7A3, // Hangul syllables
    0xFF00, 0xFFEF, // Full-width / half-width forms
    0,
];

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

#[cfg(target_os = "macos")]
const HAN_AND_KANA_FALLBACK_FONT_CANDIDATES: &[FontCandidate] = &[
    FontCandidate {
        family: "Hiragino Sans GB",
        path: "/System/Library/Fonts/Hiragino Sans GB.ttc",
    },
    FontCandidate {
        family: "CJK Symbols Fallback",
        path: "/System/Library/Fonts/CJKSymbolsFallback.ttc",
    },
    FontCandidate {
        family: "STHeiti Medium",
        path: "/System/Library/Fonts/STHeiti Medium.ttc",
    },
];

#[cfg(target_os = "macos")]
const HANGUL_FALLBACK_FONT_CANDIDATES: &[FontCandidate] = &[
    FontCandidate {
        family: "Apple SD Gothic Neo",
        path: "/System/Library/Fonts/AppleSDGothicNeo.ttc",
    },
    FontCandidate {
        family: "CJK Symbols Fallback",
        path: "/System/Library/Fonts/CJKSymbolsFallback.ttc",
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

#[cfg(target_os = "windows")]
const HAN_AND_KANA_FALLBACK_FONT_CANDIDATES: &[FontCandidate] = &[
    FontCandidate {
        family: "Yu Gothic",
        path: "C:\\Windows\\Fonts\\YuGothM.ttc",
    },
    FontCandidate {
        family: "Meiryo",
        path: "C:\\Windows\\Fonts\\meiryo.ttc",
    },
    FontCandidate {
        family: "Microsoft YaHei",
        path: "C:\\Windows\\Fonts\\msyh.ttc",
    },
];

#[cfg(target_os = "windows")]
const HANGUL_FALLBACK_FONT_CANDIDATES: &[FontCandidate] = &[FontCandidate {
    family: "Malgun Gothic",
    path: "C:\\Windows\\Fonts\\malgun.ttf",
}];

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

#[cfg(all(unix, not(target_os = "macos")))]
const HAN_AND_KANA_FALLBACK_FONT_CANDIDATES: &[FontCandidate] = &[
    FontCandidate {
        family: "Noto Sans CJK",
        path: "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    },
    FontCandidate {
        family: "Noto Sans CJK",
        path: "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc",
    },
    FontCandidate {
        family: "Droid Sans Fallback",
        path: "/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
    },
];

#[cfg(all(unix, not(target_os = "macos")))]
const HANGUL_FALLBACK_FONT_CANDIDATES: &[FontCandidate] = &[
    FontCandidate {
        family: "Noto Sans CJK",
        path: "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    },
    FontCandidate {
        family: "Noto Sans CJK",
        path: "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc",
    },
];

#[cfg(not(any(target_os = "macos", target_os = "windows", unix)))]
const SYSTEM_MONOSPACE_FONT_CANDIDATES: &[FontCandidate] = &[];

#[cfg(not(any(target_os = "macos", target_os = "windows", unix)))]
const HAN_AND_KANA_FALLBACK_FONT_CANDIDATES: &[FontCandidate] = &[];

#[cfg(not(any(target_os = "macos", target_os = "windows", unix)))]
const HANGUL_FALLBACK_FONT_CANDIDATES: &[FontCandidate] = &[];

fn try_load_primary_font(
    fonts: &mut FontAtlas,
    font_cfg: &FontConfig,
    verbose_logging: bool,
) -> bool {
    for candidate in SYSTEM_MONOSPACE_FONT_CANDIDATES {
        if !Path::new(candidate.path).is_file() {
            continue;
        }

        if fonts
            .add_font_from_file_ttf(candidate.path, DEFAULT_UI_FONT_SIZE, Some(font_cfg), None)
            .is_some()
        {
            if verbose_logging {
                eprintln!(
                    "Using system UI font: {} ({})",
                    candidate.family, candidate.path
                );
            }
            return true;
        }
    }

    false
}

fn try_merge_fallback_font(
    fonts: &mut FontAtlas,
    candidates: &[FontCandidate],
    glyph_ranges: &'static [sys::ImWchar],
    label: &str,
    font_cfg: &FontConfig,
    verbose_logging: bool,
) -> bool {
    for candidate in candidates {
        if !Path::new(candidate.path).is_file() {
            continue;
        }

        if fonts
            .add_font_from_file_ttf(
                candidate.path,
                DEFAULT_UI_FONT_SIZE,
                Some(font_cfg),
                Some(glyph_ranges),
            )
            .is_some()
        {
            if verbose_logging {
                eprintln!(
                    "Merged {label} fallback font: {} ({})",
                    candidate.family, candidate.path,
                );
            }
            return true;
        }
    }

    false
}

pub fn configure_imgui_fonts(context: &mut Context, verbose_logging: bool) {
    let mut fonts = context.fonts();
    fonts.clear();

    let primary_debug_name = format!("s6ui UI {:.1}px", DEFAULT_UI_FONT_SIZE);
    let primary_font_cfg = FontConfig::new()
        .name(&primary_debug_name)
        .oversample_h(2)
        .oversample_v(2);

    if !try_load_primary_font(&mut fonts, &primary_font_cfg, verbose_logging) {
        fonts.add_font_default(Some(&primary_font_cfg));
        eprintln!(
            "Failed to locate a preferred system monospace font; falling back to Dear ImGui default"
        );
    }

    let fallback_debug_name = format!("s6ui CJK fallback {:.1}px", DEFAULT_UI_FONT_SIZE);
    let fallback_font_cfg = FontConfig::new()
        .name(&fallback_debug_name)
        .merge_mode(true)
        .oversample_h(2)
        .oversample_v(2);

    let han_and_kana_loaded = try_merge_fallback_font(
        &mut fonts,
        HAN_AND_KANA_FALLBACK_FONT_CANDIDATES,
        HAN_AND_KANA_GLYPH_RANGES,
        "CJK",
        &fallback_font_cfg,
        verbose_logging,
    );
    let hangul_loaded = try_merge_fallback_font(
        &mut fonts,
        HANGUL_FALLBACK_FONT_CANDIDATES,
        HANGUL_GLYPH_RANGES,
        "Hangul",
        &fallback_font_cfg,
        verbose_logging,
    );

    if !han_and_kana_loaded && !hangul_loaded {
        eprintln!("No system CJK fallback font found; rare CJK glyphs may still be missing");
    }
}
