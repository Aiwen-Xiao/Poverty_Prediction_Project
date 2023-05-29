from mpi4py import MPI
import os
import cv2
import numpy as np
import pandas as pd

# Initialize MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

path = "satellite_images_1"
file_list = os.listdir(path)

# Divide the file list into equal parts
num_files_per_process = len(file_list) // size
file_list_per_process = [file_list[i*num_files_per_process:(i+1)*num_files_per_process] for i in range(size)]

# Scatter file lists to all processes
file_list_per_process = comm.scatter(file_list_per_process, root=0)

def increase_contrast(image, alpha, beta):
    # Apply contrast adjustment
    adjusted_image = cv2.convertScaleAbs(image, alpha=alpha, beta=beta)
    return adjusted_image

results_per_process = []
img_id_lst = []
for img in file_list_per_process:
    img_id = int(img.split(".")[0])
    img_path = os.path.join(path, img)
    img = cv2.imread(img_path)
    img = increase_contrast(img, alpha=2, beta=2)
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    
    white_lower = np.array([0, 0, 200], np.uint8)
    white_upper = np.array([255, 30, 255], np.uint8)
    white = cv2.inRange(hsv, white_lower, white_upper)
    # Morphological Transform, Dilation

    kernal = np.ones((5, 5), "uint8")
    white = cv2.dilate(white, kernal)
    res_black = cv2.bitwise_and(img, img, mask=white)

    (contours, hierarchy) = cv2.findContours(white, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    cnts = sorted(contours, key=cv2.contourArea, reverse=True)[:5]  # get largest  contour area

    total_area = 0
    for pic, contour in enumerate(cnts):
        approx = cv2.approxPolyDP(contour, 0.01 * cv2.arcLength(contour, True), True)
        area = cv2.contourArea(contour)
        if area > 1500:
            total_area += area
            bbbbb = cv2.drawContours(img, [approx], -1, (0, 255, 0), 3)
    
    results_per_process.append(total_area)
    img_id_lst.append(img_id)

# Gather the results from all processes
all_results = comm.gather(results_per_process, root=0)
img_ids = comm.gather(img_id_lst, root=0)

# Consolidate the results in the root process
if rank == 0:
    final_results = [item for sublist in all_results for item in sublist]
    final_img_ids = [item for sublist in img_ids for item in sublist]
    df = pd.DataFrame({"image_id":final_img_ids, "area":final_results})
    df.to_csv("imageVsArea.csv")
    
# sbatch mpi.sbatch