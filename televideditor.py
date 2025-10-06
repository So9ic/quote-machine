import os
import time
import requests
import json
import logging
import subprocess
from PIL import Image, ImageDraw, ImageFont, ImageFilter
import textwrap

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# --- Constants and Configuration ---
WORKER_PUBLIC_URL = os.environ.get("WORKER_PUBLIC_URL")
RAILWAY_API_TOKEN = os.environ.get("RAILWAY_API_TOKEN")  # <-- Add this
RAILWAY_SERVICE_ID = os.environ.get("RAILWAY_SERVICE_ID")  # <-- Add this
# Add these to your Constants section
UPSTASH_REDIS_REST_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")


# --- ORIGINAL Video Processing Constants ---
COMP_WIDTH = 1080
COMP_HEIGHT = 1920
COMP_SIZE_STR = f"{COMP_WIDTH}x{COMP_HEIGHT}"
BACKGROUND_COLOR = "black"
FPS = 30
IMAGE_DURATION = 8
MEDIA_FADE_DURATION = 10
CAPTION_FADE_DURATION = 4
MEDIA_Y_OFFSET = 0
CAPTION_V_PADDING = 37
CAPTION_FONT_SIZE = 40
CAPTION_TOP_PADDING_LINES = 0
CAPTION_LINE_SPACING = 5
CAPTION_FONT = "ZalandoSans-Medium"
CAPTION_TEXT_COLOR = (255, 255, 255)
CAPTION_BG_COLOR = (255, 255, 255)
SHADOW_COLOR = (0, 0, 0)
SHADOW_OFFSET = (0, 0)
SHADOW_BLUR_RADIUS = 20

# --- File Paths ---
DOWNLOAD_PATH = "downloads"
OUTPUT_PATH = "outputs"

# --- Helper Functions ---
def cleanup_files(file_list):
    for file_path in file_list:
        if file_path and os.path.exists(file_path):
            try: os.remove(file_path)
            except OSError as e: logging.error(f"Error deleting file {file_path}: {e}")

def create_directories():
    for path in [DOWNLOAD_PATH, OUTPUT_PATH]:
        if not os.path.exists(path): os.makedirs(path)

# --- Railway API Functions ---
def stop_railway_deployment():
    logging.info("Attempting to stop Railway deployment...")
    api_token, service_id = os.environ.get("RAILWAY_API_TOKEN"), os.environ.get("RAILWAY_SERVICE_ID")
    if not api_token or not service_id:
        logging.warning("RAILWAY variables not set. Skipping stop.")
        return
    graphql_url, headers = "https://backboard.railway.app/graphql/v2", {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
    get_id_query = {"query": "query getLatestDeployment($serviceId: String!) { service(id: $serviceId) { deployments(first: 1) { edges { node { id } } } } }", "variables": {"serviceId": service_id}}
    try:
        response = requests.post(graphql_url, json=get_id_query, headers=headers, timeout=15)
        response.raise_for_status()
        edges = response.json().get('data', {}).get('service', {}).get('deployments', {}).get('edges', [])
        if not edges:
             logging.warning("No active deployments found to stop.")
             return
        deployment_id = edges[0]['node']['id']
        logging.info(f"Fetched latest deployment ID: {deployment_id}")
    except Exception as e:
        logging.error(f"Failed to get Railway deployment ID: {e}")
        return
    stop_mutation = {"query": "mutation deploymentStop($id: String!) { deploymentStop(id: $id) }", "variables": {"id": deployment_id}}
    try:
        response = requests.post(graphql_url, json=stop_mutation, headers=headers, timeout=15)
        response.raise_for_status()
        logging.info("Successfully sent stop command to Railway.")
    except Exception as e:
        logging.error(f"Failed to send stop command: {e}")

# --- Worker Communication ---
def fetch_job_from_redis():
    url = f"{UPSTASH_REDIS_REST_URL}/rpop/job_queue"
    headers = {"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        result = response.json().get("result")
        return json.loads(result) if result else None
    except Exception as e:
        logging.error(f"Redis fetch failed: {e}")
        return None

def submit_result_to_worker(job_data, video_path):
    url = f"{WORKER_PUBLIC_URL}/submit-result"
    logging.info(f"Submitting result for job {job_data['job_id']}...")
    try:
        with open(video_path, 'rb') as video_file:
            files = {'video': ('final_video.mp4', video_file, 'video/mp4'), 'job_data': (None, json.dumps(job_data), 'application/json')}
            response = requests.post(url, files=files, timeout=300)
            response.raise_for_status()
        logging.info("Successfully submitted result to worker.")
    except Exception as e:
        logging.error(f"Error submitting to worker: {e}")

# --- Core Processing Logic ---
def download_file_from_url(url, save_path):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        with requests.get(url, stream=True, timeout=60, headers=headers) as r:
            r.raise_for_status()
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
        logging.info(f"Downloaded: {save_path}")
        return save_path
    except Exception as e:
        logging.error(f"Download failed for {url}: {e}")
        return None

def create_caption_image(text, job_id):
    padded_text = ("\n" * CAPTION_TOP_PADDING_LINES) + text
    font_path = f"{CAPTION_FONT}.ttf"
    if not os.path.exists(font_path): raise FileNotFoundError(f"Font file not found: {font_path}")
    font = ImageFont.truetype(font_path, CAPTION_FONT_SIZE)
    wrapped_text = "\n".join([item for line in padded_text.split('\n') for item in textwrap.wrap(line, width=35, break_long_words=True) or ['']])
    dummy_draw = ImageDraw.Draw(Image.new('RGB', (0,0)))
    text_bbox = dummy_draw.multiline_textbbox((0, 0), wrapped_text, font=font, align="center", spacing=CAPTION_LINE_SPACING, stroke_width=1)
    text_width, text_height = int(text_bbox[2] - text_bbox[0]), int(text_bbox[3] - text_bbox[1])
    img_padding = SHADOW_BLUR_RADIUS * 4
    img_width, img_height = text_width + img_padding, text_height + img_padding
    shadow_img = Image.new('RGBA', (img_width, img_height), (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow_img)
    shadow_pos = (img_padding / 2 + SHADOW_OFFSET[0], img_padding / 2 + SHADOW_OFFSET[1])
    shadow_draw.multiline_text(shadow_pos, wrapped_text, font=font, fill=SHADOW_COLOR, anchor="la", align="center", spacing=CAPTION_LINE_SPACING, stroke_width=2, stroke_fill=SHADOW_COLOR)
    shadow_img = shadow_img.filter(ImageFilter.GaussianBlur(radius=SHADOW_BLUR_RADIUS))
    final_draw = ImageDraw.Draw(shadow_img)
    text_pos = (img_padding / 2, img_padding / 2)
    final_draw.multiline_text(text_pos, wrapped_text, font=font, fill=CAPTION_TEXT_COLOR, anchor="la", align="center", spacing=CAPTION_LINE_SPACING, stroke_width=2, stroke_fill=(0, 0, 0))
    caption_image_path = os.path.join(OUTPUT_PATH, f"caption_{job_id}.png")
    shadow_img.save(caption_image_path)
    return caption_image_path


def process_video_job(job_data):
    """The main video creation logic, following your original function structure."""
    job_id = job_data['job_id']
    logging.info(f"Starting processing for job_id: {job_id}")

    files_to_clean = []
    
    try:
        # ADAPTED: Download GDrive links instead of Telegram file_id
        media_path = download_file_from_url(job_data['bg_link'], os.path.join(DOWNLOAD_PATH, f"bg_{job_id}.jpg"))
        bgm_path = download_file_from_url(job_data['bgm_link'], os.path.join(DOWNLOAD_PATH, f"bgm_{job_id}.mp3"))
        if not media_path or not bgm_path: raise ValueError("Media download failed.")
        files_to_clean.extend([media_path, bgm_path])

        # ADAPTED: Set variables as they would be for an image in your old script
        with Image.open(media_path) as img:
            media_w, media_h = img.width, img.height
        final_duration = IMAGE_DURATION # Use fixed duration for image

        # ADAPTED: Use 'quote' key from new job data
        caption_image_path = create_caption_image(job_data['quote'], job_id)
        files_to_clean.append(caption_image_path)

        # PRESERVED: Your original geometry calculations
        output_filepath = os.path.join(OUTPUT_PATH, f"output_{job_id}.mp4")
        scale_ratio = COMP_WIDTH / media_w
        scaled_media_h = int(media_h * scale_ratio)
        media_y_pos = int((COMP_HEIGHT / 2 - scaled_media_h / 2) + MEDIA_Y_OFFSET)

        # --- FFmpeg Command Assembly (Following Your Original Structure) ---
        command = [
            'ffmpeg', '-y',
            # Input 0: Background (Your original)
            '-f', 'lavfi', '-i', f'color=c={BACKGROUND_COLOR}:s={COMP_SIZE_STR}:d={final_duration}',
            # Input 1: Media (Image, looped as per your original)
            '-loop', '1', '-t', str(final_duration), '-i', media_path,
            # Input 2: Caption Image (looped as per your original)
            '-loop', '1', '-i', caption_image_path,
            # ADAPTED: Add BGM as a new input
            '-i', bgm_path
        ]
        
        filter_parts = []
        
        # PRESERVED: Your original filter for media scaling and fade
        media_fade_filter = f",fade=t=in:st=0:d={MEDIA_FADE_DURATION}"
        filter_parts.append(f"[1:v]scale={COMP_WIDTH}:-1,setpts=PTS-STARTPTS{media_fade_filter}[scaled_media]")
        
        # PRESERVED: Your original filter for the caption
        caption_fade_filter = f",fade=t=in:st=0:d={CAPTION_FADE_DURATION}"
        filter_parts.append(f"[2:v]format=rgba,trim=duration={final_duration}{caption_fade_filter}[faded_caption]")

        # PRESERVED: Your original overlay logic
        filter_parts.extend([
            f"[0:v][scaled_media]overlay=(W-w)/2:{media_y_pos}[base_scene]",
            f"[base_scene][faded_caption]overlay=(W-w)/2:(H-h)/2[final_v]"
        ])
        
        # ADAPTED: Add a filter for the new audio stream
        audio_filter = f"[3:a]atrim=0:{final_duration},afade=t=out:st={final_duration-2}:d=2[final_a]"
        filter_parts.append(audio_filter)

        filter_complex = ";".join(filter_parts)

        # ADAPTED: Add the new audio stream to the map
        map_args = ['-map', '[final_v]', '-map', '[final_a]']
        
        # PRESERVED: Your original encoding options and trim fix
        command.extend([
            '-filter_complex', filter_complex,
            *map_args,
            '-ss', '0.4', # Your trim fix
            '-c:v', 'libx264',
            '-preset', 'fast', '-tune', 'zerolatency',
            '-c:a', 'aac', '-b:a', '192k',
            '-r', str(FPS),
            '-pix_fmt', 'yuv420p',
            '-t', str(final_duration), # Explicitly set duration
            output_filepath
        ])
        
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logging.error(f"FFMPEG STDERR: {result.stderr}")
            raise subprocess.CalledProcessError(result.returncode, command, stderr=result.stderr)
        
        logging.info(f"FFmpeg processing finished for job {job_id}.")
        files_to_clean.append(output_filepath)

        # ADAPTED: Removed frame extraction and updated submit call for the new worker
        submit_result_to_worker(job_data, output_filepath)

    except Exception as e:
        error_snippet = str(e)[-1000:]
        logging.error(f"Failed to process job {job_id}: {error_snippet}", exc_info=True)
    
    finally:
        logging.info(f"Cleaning up files for job {job_id}.")
        cleanup_files(files_to_clean)

# --- Main Bot Loop ---
if __name__ == '__main__':
    logging.info("Starting Python Job Processor...")
    create_directories()
    job = fetch_job_from_redis()
    if job:
        logging.info("Job found. Processing...")
        process_video_job(job)
    else:
        logging.info("No job found in queue.")
    logging.info("Task complete. Requesting shutdown.")
    stop_railway_deployment()
