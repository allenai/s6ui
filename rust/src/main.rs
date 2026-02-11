mod aws;
mod backend;
mod events;
mod model;
mod preview;
mod text_viewer;
mod ui;

use aws::credentials;
use aws::s3_backend::S3Backend;
use dear_imgui_rs::*;
use dear_imgui_wgpu::WgpuRenderer;
use dear_imgui_winit::WinitPlatform;
use model::BrowserModel;
use pollster::block_on;
use std::{sync::Arc, time::Instant};
use ui::BrowserUI;
use winit::{
    application::ApplicationHandler,
    dpi::LogicalSize,
    event::WindowEvent,
    event_loop::{ActiveEventLoop, ControlFlow, EventLoop, EventLoopProxy},
    window::{Window, WindowId},
};

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
    show_debug_window: bool,
    initial_path: Option<String>,
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

        Self {
            window: None,
            model,
            browser_ui: BrowserUI::new(),
            runtime,
            event_proxy: Some(event_proxy),
            backend_initialized: false,
            show_debug_window: opts.debug,
            initial_path: opts.initial_path,
        }
    }

    fn init_backend(&mut self) {
        if self.backend_initialized || self.model.profiles.is_empty() {
            return;
        }

        let profile = self.model.profiles[self.model.selected_profile_idx].clone();
        if let Some(proxy) = self.event_proxy.clone() {
            let backend = S3Backend::new(profile, self.runtime.handle().clone(), proxy);
            self.model.set_backend(Box::new(backend));

            if let Some(path) = self.initial_path.take() {
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
            let backend = S3Backend::new(profile, self.runtime.handle().clone(), proxy);
            self.model.set_backend(Box::new(backend));
            self.model.refresh();
        }
    }
}

impl AppWindow {
    fn new(event_loop: &ActiveEventLoop) -> Result<Self, Box<dyn std::error::Error>> {
        let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
            backends: wgpu::Backends::PRIMARY,
            ..Default::default()
        });

        let window = Arc::new(
            event_loop.create_window(
                Window::default_attributes()
                    .with_title("s6ui - S3 Browser")
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

        let (device, queue) =
            block_on(adapter.request_device(&wgpu::DeviceDescriptor::default()))?;

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

        let mut platform = WinitPlatform::new(&mut context);
        platform.attach_window(&window, dear_imgui_winit::HiDpiMode::Default, &mut context);

        let init_info =
            dear_imgui_wgpu::WgpuInitInfo::new(device.clone(), queue.clone(), surface_desc.format);
        let mut renderer = WgpuRenderer::new(init_info, &mut context)
            .expect("Failed to initialize WGPU renderer");
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
    ) -> Result<(), Box<dyn std::error::Error>> {
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
                return Ok(());
            }
            Err(wgpu::SurfaceError::Timeout) => return Ok(()),
            Err(e) => return Err(Box::new(e)),
        };

        self.imgui
            .platform
            .prepare_frame(&self.window, &mut self.imgui.context);
        let ui = self.imgui.context.frame();

        // Process backend events
        model.process_events();

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
        Ok(())
    }
}

impl ApplicationHandler<()> for App {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        if self.window.is_none() {
            match AppWindow::new(event_loop) {
                Ok(window) => {
                    self.window = Some(window);
                    self.init_backend();
                }
                Err(e) => {
                    eprintln!("Failed to create window: {e}");
                    event_loop.exit();
                }
            }
        }
    }

    fn user_event(&mut self, _event_loop: &ActiveEventLoop, _event: ()) {
        // Backend pushed events - request redraw
        if let Some(window) = &self.window {
            window.window.request_redraw();
        }
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

        match event {
            WindowEvent::Resized(size) => {
                let window = self.window.as_mut().unwrap();
                window.resize(size);
                window.window.request_redraw();
            }
            WindowEvent::ScaleFactorChanged { .. } => {
                let window = self.window.as_mut().unwrap();
                let new_size = window.window.inner_size();
                window.resize(new_size);
                window.window.request_redraw();
            }
            WindowEvent::CloseRequested => event_loop.exit(),
            WindowEvent::RedrawRequested => {
                let prev_profile = self.model.selected_profile_idx;

                {
                    let window = self.window.as_mut().unwrap();
                    if let Err(e) =
                        window.render(&mut self.model, &mut self.browser_ui, self.show_debug_window)
                    {
                        eprintln!("Render error: {e}");
                    }
                }

                if self.model.selected_profile_idx != prev_profile {
                    self.recreate_backend();
                }

                self.window.as_ref().unwrap().window.request_redraw();
            }
            _ => {}
        }
    }

    fn about_to_wait(&mut self, _event_loop: &ActiveEventLoop) {
        if let Some(window) = &self.window {
            window.window.request_redraw();
        }
    }
}

fn main() {
    let opts = parse_args();

    if opts.verbose {
        eprintln!("s6ui {} - verbose mode enabled", env!("CARGO_PKG_VERSION"));
    }

    let event_loop = EventLoop::<()>::with_user_event().build().unwrap();
    event_loop.set_control_flow(ControlFlow::Poll);

    let proxy = event_loop.create_proxy();
    let mut app = App::new(proxy, opts);
    event_loop.run_app(&mut app).unwrap();
}
