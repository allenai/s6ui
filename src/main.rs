mod app_clipboard;
mod aws;
mod backend;
mod events;
mod jsonl_viewer;
mod model;
mod preview;
mod settings;
mod text_viewer;
mod ui;
mod ui_fonts;
mod warc_viewer;

use app_clipboard::SystemClipboardBackend;
use aws::credentials;
use aws::s3_backend::S3Backend;
use dear_imgui_rs::*;
use dear_imgui_wgpu::WgpuRenderer;
use dear_imgui_winit::WinitPlatform;
use model::BrowserModel;
use pollster::block_on;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use ui::BrowserUI;
use ui_fonts::configure_imgui_fonts;
use winit::{
    application::ApplicationHandler,
    dpi::LogicalSize,
    event::{StartCause, WindowEvent},
    event_loop::{ActiveEventLoop, ControlFlow, EventLoop, EventLoopProxy},
    window::{Window, WindowId},
};

const ACTIVE_REDRAW_INTERVAL: Duration = Duration::from_millis(16);
const IDLE_REDRAW_INTERVAL: Duration = Duration::from_millis(500);
const APP_WINDOW_TITLE: &str = concat!("s6ui ", env!("CARGO_PKG_VERSION"), " - S3 Browser");

struct ImguiState {
    context: Context,
    platform: WinitPlatform,
    renderer: WgpuRenderer,
    clear_color: wgpu::Color,
    last_frame: Instant,
}

struct AppWindow {
    device: wgpu::Device,
    queue: wgpu::Queue,
    window: Arc<Window>,
    surface_desc: wgpu::SurfaceConfiguration,
    surface: wgpu::Surface<'static>,
    imgui: ImguiState,
}

/// Parsed command-line options
struct CliOptions {
    verbose: bool,
    debug: bool,
    initial_path: Option<String>,
}

fn parse_args() -> CliOptions {
    let args: Vec<String> = std::env::args().collect();

    // First pass: check for --version
    for arg in &args[1..] {
        if arg == "--version" {
            let version = env!("CARGO_PKG_VERSION");
            println!("s6ui {version}");
            std::process::exit(0);
        }
    }

    let mut opts = CliOptions {
        verbose: false,
        debug: false,
        initial_path: None,
    };

    for arg in &args[1..] {
        match arg.as_str() {
            "-v" | "--verbose" => opts.verbose = true,
            "-d" | "--debug" => opts.debug = true,
            a if a.starts_with("s3://") || a.starts_with("s3:") => {
                opts.initial_path = Some(a.to_string());
            }
            other => {
                eprintln!("Unknown argument: {other}");
                eprintln!("Usage: s6ui [OPTIONS] [s3://bucket/prefix]");
                eprintln!("  --version       Show version and exit");
                eprintln!("  -v, --verbose   Enable verbose logging to stderr");
                eprintln!("  -d, --debug     Show ImGui debug/metrics window");
                std::process::exit(1);
            }
        }
    }

    opts
}

struct App {
    window: Option<AppWindow>,
    model: BrowserModel,
    browser_ui: BrowserUI,
    runtime: tokio::runtime::Runtime,
    event_proxy: Option<EventLoopProxy<()>>,
    backend_initialized: bool,
    verbose_logging: bool,
    show_debug_window: bool,
    initial_path: Option<String>,
    had_activity: bool,
    activity_since_last_render: bool,
}

impl App {
    fn new(event_proxy: EventLoopProxy<()>, opts: CliOptions) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        let mut model = BrowserModel::new();
        model.profiles = credentials::load_aws_profiles();
        model.selected_profile_idx = credentials::default_profile_index(&model.profiles);
        let saved_settings = settings::load_settings();
        if opts.initial_path.is_none() && !saved_settings.profile_name.is_empty() {
            if let Some(idx) = model
                .profiles
                .iter()
                .position(|profile| profile.name == saved_settings.profile_name)
            {
                model.selected_profile_idx = idx;
            }
        }
        model.set_settings(saved_settings);

        Self {
            window: None,
            model,
            browser_ui: BrowserUI::new(),
            runtime,
            event_proxy: Some(event_proxy),
            backend_initialized: false,
            verbose_logging: opts.verbose,
            show_debug_window: opts.debug,
            initial_path: opts.initial_path,
            had_activity: true,
            activity_since_last_render: true,
        }
    }

    fn init_backend(&mut self) {
        if self.backend_initialized || self.model.profiles.is_empty() {
            return;
        }

        let profile = self.model.profiles[self.model.selected_profile_idx].clone();
        if let Some(proxy) = self.event_proxy.clone() {
            let backend = S3Backend::new(
                profile,
                self.runtime.handle().clone(),
                proxy,
                self.verbose_logging,
            );
            self.model.set_backend(Box::new(backend));

            if let Some(path) = self.initial_path.take() {
                self.model.navigate_to(&path);
            } else if !self.model.settings().bucket.is_empty() {
                let path = format!(
                    "s3://{}/{}",
                    self.model.settings().bucket,
                    self.model.settings().prefix
                );
                self.model.navigate_to(&path);
            } else {
                self.model.refresh();
            }
            self.backend_initialized = true;
        }
    }

    fn recreate_backend(&mut self) {
        if self.model.profiles.is_empty() {
            return;
        }
        let profile = self.model.profiles[self.model.selected_profile_idx].clone();
        if let Some(proxy) = self.event_proxy.clone() {
            let backend = S3Backend::new(
                profile,
                self.runtime.handle().clone(),
                proxy,
                self.verbose_logging,
            );
            self.model.set_backend(Box::new(backend));
            self.model.refresh();
        }
    }

    fn persist_settings(&mut self) {
        let profile_name = self
            .model
            .profiles
            .get(self.model.selected_profile_idx)
            .map(|profile| profile.name.clone())
            .unwrap_or_default();
        let bucket = self.model.current_bucket.clone();
        let prefix = self.model.current_prefix.clone();

        let settings = self.model.settings_mut();
        settings.profile_name = profile_name;
        settings.bucket = bucket;
        settings.prefix = prefix;

        if let Err(err) = settings::save_settings(settings) {
            eprintln!("Failed to save settings: {err}");
        }
    }

    fn mark_activity(&mut self) {
        self.activity_since_last_render = true;
    }

    fn request_redraw(&self) {
        if let Some(window) = &self.window {
            window.window.request_redraw();
        }
    }

    fn redraw_interval(&self) -> Duration {
        if self.had_activity {
            ACTIVE_REDRAW_INTERVAL
        } else {
            IDLE_REDRAW_INTERVAL
        }
    }
}

impl AppWindow {
    fn new(
        event_loop: &ActiveEventLoop,
        verbose_logging: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
            backends: wgpu::Backends::PRIMARY,
            ..Default::default()
        });

        let window = Arc::new(
            event_loop.create_window(
                Window::default_attributes()
                    .with_title(APP_WINDOW_TITLE)
                    .with_inner_size(LogicalSize::new(1280.0, 720.0)),
            )?,
        );

        let surface = instance.create_surface(window.clone())?;

        let adapter = block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
            power_preference: wgpu::PowerPreference::HighPerformance,
            compatible_surface: Some(&surface),
            force_fallback_adapter: false,
        }))
        .expect("Failed to find an appropriate adapter");

        let (device, queue) = block_on(adapter.request_device(&wgpu::DeviceDescriptor::default()))?;

        let physical_size = window.inner_size();
        let caps = surface.get_capabilities(&adapter);
        let preferred_srgb = [
            wgpu::TextureFormat::Bgra8UnormSrgb,
            wgpu::TextureFormat::Rgba8UnormSrgb,
        ];
        let format = preferred_srgb
            .iter()
            .cloned()
            .find(|f| caps.formats.contains(f))
            .unwrap_or(caps.formats[0]);

        let surface_desc = wgpu::SurfaceConfiguration {
            usage: wgpu::TextureUsages::RENDER_ATTACHMENT,
            format,
            width: physical_size.width,
            height: physical_size.height,
            present_mode: wgpu::PresentMode::Fifo,
            alpha_mode: wgpu::CompositeAlphaMode::Auto,
            view_formats: vec![],
            desired_maximum_frame_latency: 2,
        };

        surface.configure(&device, &surface_desc);

        let mut context = Context::create();
        context.set_ini_filename(None::<String>).unwrap();
        context.set_clipboard_backend(SystemClipboardBackend::new());

        let mut platform = WinitPlatform::new(&mut context);
        platform.attach_window(&window, dear_imgui_winit::HiDpiMode::Default, &mut context);
        configure_imgui_fonts(&mut context, verbose_logging);

        let init_info =
            dear_imgui_wgpu::WgpuInitInfo::new(device.clone(), queue.clone(), surface_desc.format);
        let mut renderer =
            WgpuRenderer::new(init_info, &mut context).expect("Failed to initialize WGPU renderer");
        renderer.set_gamma_mode(dear_imgui_wgpu::GammaMode::Auto);

        let imgui = ImguiState {
            context,
            platform,
            renderer,
            clear_color: wgpu::Color {
                r: 0.1,
                g: 0.2,
                b: 0.3,
                a: 1.0,
            },
            last_frame: Instant::now(),
        };

        Ok(Self {
            device,
            queue,
            window,
            surface_desc,
            surface,
            imgui,
        })
    }

    fn resize(&mut self, new_size: winit::dpi::PhysicalSize<u32>) {
        if new_size.width > 0 && new_size.height > 0 {
            self.surface_desc.width = new_size.width;
            self.surface_desc.height = new_size.height;
            self.surface.configure(&self.device, &self.surface_desc);
        }
    }

    fn render(
        &mut self,
        model: &mut BrowserModel,
        browser_ui: &mut BrowserUI,
        show_debug_window: bool,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let now = Instant::now();
        let delta_time = now - self.imgui.last_frame;
        self.imgui
            .context
            .io_mut()
            .set_delta_time(delta_time.as_secs_f32());
        self.imgui.last_frame = now;

        let frame = match self.surface.get_current_texture() {
            Ok(frame) => frame,
            Err(wgpu::SurfaceError::Lost | wgpu::SurfaceError::Outdated) => {
                self.surface.configure(&self.device, &self.surface_desc);
                return Ok(false);
            }
            Err(wgpu::SurfaceError::Timeout) => return Ok(false),
            Err(e) => return Err(Box::new(e)),
        };

        // Process backend events
        let had_backend_events = model.process_events();

        // Upload/refresh image-WARC gallery textures before the ImGui frame, where
        // the wgpu device/queue/renderer are reachable (the in-frame `ui` borrows
        // self.imgui.context, so we touch self.imgui.renderer here instead).
        browser_ui.sync_warc_textures(
            model,
            &self.device,
            &self.queue,
            &mut self.imgui.renderer,
        );

        self.imgui
            .platform
            .prepare_frame(&self.window, &mut self.imgui.context);
        let ui = self.imgui.context.frame();

        // Get window size for UI
        let size = self.window.inner_size();
        let scale = self.imgui.platform.hidpi_factor();
        let window_size = [
            size.width as f32 / scale as f32,
            size.height as f32 / scale as f32,
        ];

        // Render browser UI
        browser_ui.render(ui, model, window_size);

        if show_debug_window {
            let mut open = true;
            ui.show_metrics_window(&mut open);
        }

        let view = frame
            .texture
            .create_view(&wgpu::TextureViewDescriptor::default());
        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("Render Encoder"),
            });

        let draw_data = self.imgui.context.render();

        {
            let mut rpass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("ImGui Render Pass"),
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view: &view,
                    resolve_target: None,
                    ops: wgpu::Operations {
                        load: wgpu::LoadOp::Clear(self.imgui.clear_color),
                        store: wgpu::StoreOp::Store,
                    },
                    depth_slice: None,
                })],
                depth_stencil_attachment: None,
                timestamp_writes: None,
                occlusion_query_set: None,
                multiview_mask: None,
            });

            self.imgui
                .renderer
                .new_frame()
                .expect("Failed to prepare new frame");

            self.imgui
                .renderer
                .render_draw_data(draw_data, &mut rpass)?;
        }

        self.queue.submit(Some(encoder.finish()));
        frame.present();
        Ok(had_backend_events)
    }
}

impl ApplicationHandler<()> for App {
    fn new_events(&mut self, _event_loop: &ActiveEventLoop, cause: StartCause) {
        if matches!(cause, StartCause::ResumeTimeReached { .. }) {
            self.request_redraw();
        }
    }

    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        if self.window.is_none() {
            match AppWindow::new(event_loop, self.verbose_logging) {
                Ok(window) => {
                    self.window = Some(window);
                    self.init_backend();
                    self.request_redraw();
                }
                Err(e) => {
                    eprintln!("Failed to create window: {e}");
                    event_loop.exit();
                }
            }
        }
    }

    fn user_event(&mut self, _event_loop: &ActiveEventLoop, _event: ()) {
        // Backend pushed events - wake immediately and stay in active redraw mode.
        self.mark_activity();
        self.request_redraw();
    }

    fn window_event(
        &mut self,
        event_loop: &ActiveEventLoop,
        _window_id: WindowId,
        event: WindowEvent,
    ) {
        if self.window.is_none() {
            return;
        }

        // Handle platform events
        {
            let window = self.window.as_mut().unwrap();
            window.imgui.platform.handle_window_event(
                &mut window.imgui.context,
                &window.window,
                &event,
            );
        }

        let event_is_activity = matches!(
            event,
            WindowEvent::Resized(_)
                | WindowEvent::ScaleFactorChanged { .. }
                | WindowEvent::KeyboardInput { .. }
                | WindowEvent::ModifiersChanged(_)
                | WindowEvent::Ime(_)
                | WindowEvent::CursorMoved { .. }
                | WindowEvent::CursorEntered { .. }
                | WindowEvent::CursorLeft { .. }
                | WindowEvent::MouseWheel { .. }
                | WindowEvent::MouseInput { .. }
                | WindowEvent::PinchGesture { .. }
                | WindowEvent::PanGesture { .. }
                | WindowEvent::DoubleTapGesture { .. }
                | WindowEvent::RotationGesture { .. }
                | WindowEvent::TouchpadPressure { .. }
                | WindowEvent::AxisMotion { .. }
                | WindowEvent::Touch(_)
                | WindowEvent::Focused(_)
        );

        if event_is_activity {
            self.mark_activity();
            self.request_redraw();
        }

        match event {
            WindowEvent::Resized(size) => {
                let window = self.window.as_mut().unwrap();
                window.resize(size);
            }
            WindowEvent::ScaleFactorChanged { .. } => {
                let window = self.window.as_mut().unwrap();
                let new_size = window.window.inner_size();
                window.resize(new_size);
            }
            WindowEvent::CloseRequested => event_loop.exit(),
            WindowEvent::RedrawRequested => {
                let prev_profile = self.model.selected_profile_idx;
                let frame_had_activity = self.activity_since_last_render;
                self.activity_since_last_render = false;
                let mut had_backend_activity = false;

                {
                    let window = self.window.as_mut().unwrap();
                    match window.render(
                        &mut self.model,
                        &mut self.browser_ui,
                        self.show_debug_window,
                    ) {
                        Ok(render_had_backend_activity) => {
                            had_backend_activity = render_had_backend_activity;
                        }
                        Err(e) => {
                            eprintln!("Render error: {e}");
                        }
                    }
                }

                if self.model.selected_profile_idx != prev_profile {
                    self.mark_activity();
                    self.recreate_backend();
                }

                self.had_activity = frame_had_activity || had_backend_activity;
            }
            _ => {}
        }
    }

    fn about_to_wait(&mut self, event_loop: &ActiveEventLoop) {
        event_loop.set_control_flow(ControlFlow::wait_duration(self.redraw_interval()));
    }

    fn exiting(&mut self, _event_loop: &ActiveEventLoop) {
        self.persist_settings();
    }
}

fn main() {
    let opts = parse_args();

    if opts.verbose {
        eprintln!("s6ui {} - verbose mode enabled", env!("CARGO_PKG_VERSION"));
    }

    let event_loop = EventLoop::<()>::with_user_event().build().unwrap();

    let proxy = event_loop.create_proxy();
    let mut app = App::new(proxy, opts);
    event_loop.run_app(&mut app).unwrap();
}
