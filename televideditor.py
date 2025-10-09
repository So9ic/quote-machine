import os
import re
import time
import requests
import json
import logging
import subprocess
from PIL import Image, ImageDraw, ImageFont
import textwrap

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# --- Constants and Configuration ---
WORKER_PUBLIC_URL = os.environ.get("WORKER_PUBLIC_URL")
RAILWAY_API_TOKEN = os.environ.get("RAILWAY_API_TOKEN")
RAILWAY_SERVICE_ID = os.environ.get("RAILWAY_SERVICE_ID")
UPSTASH_REDIS_REST_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

if not all([WORKER_PUBLIC_URL, RAILWAY_API_TOKEN, RAILWAY_SERVICE_ID, UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN]):
    raise ValueError("One or more required environment variables are not set!")

# --- Video Processing Constants ---
COMP_WIDTH = 1080
COMP_HEIGHT = 1920
COMP_SIZE_STR = f"{COMP_WIDTH}x{COMP_HEIGHT}"
BACKGROUND_COLOR = "black"
FPS = 30
MEDIA_Y_OFFSET = 100
CAPTION_V_PADDING = 37
CAPTION_FONT_SIZE = 55
CAPTION_TOP_PADDING_LINES = 0
CAPTION_LINE_SPACING = 12
CAPTION_FONT = "Montserrat-ExtraBold"
CAPTION_TEXT_COLOR = (0, 0, 0)
CAPTION_BG_COLOR = (255, 255, 255)

# --- File Paths ---
DOWNLOAD_PATH = "downloads"
OUTPUT_PATH = "outputs"

# --- Helper, Railway & Worker Functions (Unchanged) ---
def cleanup_files(file_list):
    for file_path in file_list:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.info(f"Cleaned up file: {file_path}")
            except OSError as e:
                logging.error(f"Error deleting file {file_path}: {e}")

def create_directories():
    for path in [DOWNLOAD_PATH, OUTPUT_PATH]:
        if not os.path.exists(path):
            os.makedirs(path)

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

def get_video_dimensions(video_path):
    """Gets width and height from a video file."""
    try:
        command = ['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=width,height', '-of', 'json', video_path]
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
        video_data = json.loads(result.stdout)['streams'][0]
        return int(video_data['width']), int(video_data['height'])
    except Exception as e:
        logging.error(f"FFprobe failed to get video dimensions: {e}")
        return None, None

# --- NEW FUNCTION TO GET VIDEO DURATION ---
def get_video_duration(video_path):
    """Gets the duration from a video file."""
    try:
        command = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
        return float(result.stdout.strip())
    except Exception as e:
        logging.error(f"FFprobe failed to get video duration: {e}")
        return None
# --- END OF NEW FUNCTION ---

def get_audio_duration(audio_path):
    """Gets the duration from an audio file."""
    try:
        command = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', audio_path]
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
        return float(result.stdout.strip())
    except Exception as e:
        logging.error(f"FFprobe failed to get audio duration: {e}")
        return None

def create_caption_image(text, job_id):
    padded_text = ("\n" * CAPTION_TOP_PADDING_LINES) + text
    font_path = f"{CAPTION_FONT}.ttf"
    if not os.path.exists(font_path): raise FileNotFoundError(f"Font file not found: {font_path}")
    font = ImageFont.truetype(font_path, CAPTION_FONT_SIZE)
    final_lines = [item for line in padded_text.split('\n') for item in textwrap.wrap(line, width=30, break_long_words=True) or ['']]
    wrapped_text = "\n".join(final_lines)
    dummy_draw = ImageDraw.Draw(Image.new('RGB', (0,0)))
    text_bbox = dummy_draw.multiline_textbbox((0, 0), wrapped_text, font=font, align="center", spacing=CAPTION_LINE_SPACING)
    text_height = text_bbox[3] - text_bbox[1]
    rect_height = text_height + (2 * CAPTION_V_PADDING)
    img_height = int(rect_height)
    img = Image.new('RGBA', (COMP_WIDTH, img_height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.rectangle([(0, 0), (COMP_WIDTH, img_height)], fill=CAPTION_BG_COLOR)
    draw.multiline_text((COMP_WIDTH / 2, img_height / 2), wrapped_text, font=font, fill=CAPTION_TEXT_COLOR, anchor="mm", align="center", spacing=CAPTION_LINE_SPACING)
    caption_image_path = os.path.join(OUTPUT_PATH, f"caption_{job_id}.png")
    img.save(caption_image_path)
    return caption_image_path, rect_height

def process_video_job(job_data):
    job_id = job_data['job_id']
    logging.info(f"Starting processing for job_id: {job_id}")
    files_to_clean = []
    
    try:
        # 1. Download media
        media_path = download_file_from_url(job_data['bg_link'], os.path.join(DOWNLOAD_PATH, f"bg_{job_id}.mp4"))
        bgm_path = download_file_from_url(job_data['bgm_link'], os.path.join(DOWNLOAD_PATH, f"bgm_{job_id}.mp3"))
        if not media_path or not bgm_path: raise ValueError("Media or BGM download failed.")
        files_to_clean.extend([media_path, bgm_path])

        # --- MODIFIED SECTION START ---
        # 2. Get properties: BGM duration and Video duration
        media_w, media_h = get_video_dimensions(media_path)
        video_duration = get_video_duration(media_path)
        audio_duration = get_audio_duration(bgm_path)

        if not all([media_w, media_h, video_duration, audio_duration]):
            raise ValueError("Could not get media properties (dimensions or durations).")

        # Calculate final duration based on the shorter of the two media files
        final_duration = min(video_duration, audio_duration)
        logging.info(f"Video duration: {video_duration:.2f}s, Audio duration: {audio_duration:.2f}s.")
        logging.info(f"Using shorter duration for final output: {final_duration:.2f}s.")
        # --- MODIFIED SECTION END ---

        # 3. Create caption image
        caption_image_path, caption_height = create_caption_image(job_data['quote'], job_id)
        files_to_clean.append(caption_image_path)
        
        # 4. Assemble and run FFmpeg command
        output_filepath = os.path.join(OUTPUT_PATH, f"output_{job_id}.mp4")
        scale_ratio = COMP_WIDTH / media_w
        scaled_media_h = int(media_h * scale_ratio)
        media_y_pos = (COMP_HEIGHT / 2 - scaled_media_h / 2) + MEDIA_Y_OFFSET
        caption_y_pos = media_y_pos - caption_height + 1
        
        command = [
            'ffmpeg', '-y',
            '-f', 'lavfi', '-i', f'color=c={BACKGROUND_COLOR}:s={COMP_SIZE_STR}:d={final_duration}', # Base color layer
            '-stream_loop', '-1', '-i', media_path, # Loop the video input indefinitely
            '-i', caption_image_path, # Caption image
            '-i', bgm_path, # BGM audio
        ]
        
        filter_parts = [
            f"[1:v]scale={COMP_WIDTH}:-1,setpts=PTS-STARTPTS[scaled_media]",
            f"[0:v][scaled_media]overlay=(W-w)/2:{media_y_pos}[bg_with_media]",
            f"[bg_with_media][2:v]overlay=(W-w)/2:{caption_y_pos}[final_v]",
            f"[3:a]asetpts=PTS-STARTPTS[final_a]", # Always use audio from BGM (4th input)
        ]

        filter_complex = ";".join(filter_parts)
        map_args = ['-map', '[final_v]', '-map', '[final_a]']
        
        command.extend([
            '-filter_complex', filter_complex, *map_args,
            '-c:v', 'libx264', '-preset', 'superfast', '-tune', 'zerolatency',
            '-c:a', 'aac', '-b:a', '192k',
            '-r', str(FPS), '-pix_fmt', 'yuv420p',
            '-t', str(final_duration), # Cut the output to the SHORTER duration
            output_filepath
        ])
        
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logging.error(f"FFMPEG STDERR: {result.stderr}")
            raise subprocess.CalledProcessError(result.returncode, command, stderr=result.stderr)
        
        logging.info(f"FFmpeg processing finished for job {job_id}.")
        files_to_clean.append(output_filepath)
        submit_result_to_worker(job_data, output_filepath)

    except Exception as e:
        error_snippet = str(e)[-1000:]
        logging.error(f"Failed to process job {job_id}: {error_snippet}", exc_info=True)
    
    finally:
        logging.info(f"Cleaning up files for job {job_id}.")
        cleanup_files(files_to_clean)

# --- Main Bot Loop (MODIFIED for "One-and-Done" Lifecycle) ---
if __name__ == '__main__':
    logging.info("Starting Python Job Processor...")
    create_directories()

    # --- Step 1: Fetch a single job from the queue. No loop. ---
    raw_job = fetch_job_from_redis()

    # --- Step 2: Check if a job was found. ---
    if raw_job:
        logging.info("Job found in queue. Attempting to decode and process...")
        try:
            job_to_process = None

            # --- Resiliency Logic to handle malformed data ---
            # This logic ensures the bot won't crash on old, bad jobs.
            if isinstance(raw_job, list):
                if raw_job: job = raw_job[0]
                else: raise ValueError("Job was an empty list.")
            else:
                job = raw_job

            if isinstance(job, str):
                job_to_process = json.loads(job)
            elif isinstance(job, dict):
                job_to_process = job
            else:
                raise TypeError(f"Job could not be decoded. Unknown type: {type(job)}")
            # --- End of Resiliency Logic ---

            # If decoding was successful, process the single job.
            if job_to_process:
                process_video_job(job_to_process)
            else:
                logging.warning(f"Job was un-parseable after decoding. Discarding.")

        except (json.JSONDecodeError, TypeError, IndexError, ValueError) as e:
            logging.error(f"FATAL: Could not decode or process job. Discarding. Error: {e}. Original Data: {raw_job}")
    else:
        # If fetch_job_from_redis returns None, there was no job.
        logging.info("No job found in queue.")

    # --- Step 3: Immediately shut down, regardless of the outcome. ---
    logging.info("Task complete. Requesting shutdown.")
    stop_railway_deployment()
    logging.info("Processor has finished its work and is exiting.")
