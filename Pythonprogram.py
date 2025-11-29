#!/usr/bin/env python3
"""
Simple console shopping cart program

Features:
- Menu of items with stock and price
- Add items to cart (validates stock)
- Customer details input
- Delivery charge calculation based on distance
- Final bill display (and optional save to file)

Run: `python Pythonprogram.py` from the repository folder.
"""

import sys
from datetime import datetime


ITEMS = {
	1: {"name": "Apples (1 kg)", "price": 120, "stock": 10},
	2: {"name": "Bread (1 loaf)", "price": 40, "stock": 20},
	3: {"name": "Milk (1 L)", "price": 60, "stock": 15},
	4: {"name": "Rice (5 kg)", "price": 400, "stock": 5},
	5: {"name": "Eggs (12 pcs)", "price": 90, "stock": 12},
}


def clear_screen():
	print("\n" * 2)


def show_menu():
	print("===== Store Menu =====")
	for idx, info in ITEMS.items():
		print(f"{idx}. {info['name']:20} | Price: Rs {info['price']:4} | In stock: {info['stock']}")
	print("0. Checkout / Finish shopping")


def input_int(prompt, min_val=None, max_val=None):
	while True:
		try:
			v = input(prompt).strip()
			if v == "":
				print("Please enter a value.")
				continue
			n = int(v)
			if min_val is not None and n < min_val:
				print(f"Value must be at least {min_val}.")
				continue
			if max_val is not None and n > max_val:
				print(f"Value must be at most {max_val}.")
				continue
			return n
		except ValueError:
			print("Please enter a valid integer.")


def add_to_cart(cart):
	while True:
		show_menu()
		choice = input_int("Choose item number (0 to checkout): ", min_val=0)
		if choice == 0:
			return
		if choice not in ITEMS:
			print("Invalid item number. Try again.")
			continue
		item = ITEMS[choice]
		if item["stock"] <= 0:
			print(f"Sorry, {item['name']} is out of stock.")
			continue
		qty = input_int(f"Enter quantity for {item['name']} (available {item['stock']}): ", min_val=1)
		if qty > item["stock"]:
			print(f"Only {item['stock']} available. Please enter a lower quantity.")
			continue
		# add to cart
		if choice in cart:
			cart[choice]["qty"] += qty
		else:
			cart[choice] = {"name": item["name"], "price": item["price"], "qty": qty}
		item["stock"] -= qty
		print(f"Added {qty} x {item['name']} to cart.")
		cont = input("Add more items? (y/n): ").strip().lower()
		if cont != "y":
			return


def show_cart(cart):
	if not cart:
		print("Cart is empty.")
		return
	print("\n===== Your Cart =====")
	total = 0
	for idx, info in cart.items():
		subtotal = info["price"] * info["qty"]
		print(f"{info['name']:20} x {info['qty']:2} @ Rs {info['price']:4} = Rs {subtotal}")
		total += subtotal
	print(f"Items total: Rs {total}")


def get_customer_details():
	print("\nEnter customer details:")
	name = input("Full name: ").strip()
	while not name:
		name = input("Please enter a name: ").strip()
	phone = input("Phone number: ").strip()
	address = input("Address: ").strip()
	# distance in km used for delivery charge
	while True:
		try:
			dist = input("Distance from store in km (numeric): ").strip()
			distance = float(dist)
			if distance < 0:
				print("Distance cannot be negative.")
				continue
			break
		except ValueError:
			print("Enter a numeric distance (e.g., 12.5)")
	return {"name": name, "phone": phone, "address": address, "distance": distance}


def calc_delivery_charge(distance):
	# For delivery charges
	# If upto 15 km then 50 Rs
	# If 15 to 30 km then 100 rs
	# Else No delivery available
	if distance <= 15:
		return 50, True
	elif distance <= 30:
		return 100, True
	else:
		return None, False


def print_bill(cart, customer, delivery_charge, delivery_available):
	print("\n===== Final Bill =====")
	print(f"Customer: {customer['name']}")
	if customer.get('phone'):
		print(f"Phone: {customer['phone']}")
	if customer.get('address'):
		print(f"Address: {customer['address']}")
	print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
	print("\nItems:")
	items_total = 0
	for idx, info in cart.items():
		subtotal = info["price"] * info["qty"]
		print(f" - {info['name']:20} x{info['qty']:2}  Rs {info['price']:4}  = Rs {subtotal}")
		items_total += subtotal
	print(f"Items total: Rs {items_total}")
	if not delivery_available:
		print("Delivery: Not available for the provided distance.")
		print("Delivery charge: -")
		total = items_total
	else:
		print(f"Delivery charge: Rs {delivery_charge}")
		total = items_total + delivery_charge
	print(f"TOTAL PAYABLE: Rs {total}")


def save_receipt(cart, customer, delivery_charge, delivery_available):
	fname = f"receipt_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
	with open(fname, 'w', encoding='utf-8') as f:
		f.write("Final Bill\n")
		f.write(f"Customer: {customer['name']}\n")
		f.write(f"Phone: {customer.get('phone','')}\n")
		f.write(f"Address: {customer.get('address','')}\n")
		f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
		f.write("Items:\n")
		items_total = 0
		for idx, info in cart.items():
			subtotal = info['price'] * info['qty']
			f.write(f"{info['name']} x{info['qty']} @ Rs {info['price']} = Rs {subtotal}\n")
			items_total += subtotal
		f.write(f"Items total: Rs {items_total}\n")
		if not delivery_available:
			f.write("Delivery: Not available for the provided distance.\n")
			f.write("Delivery charge: -\n")
			total = items_total
		else:
			f.write(f"Delivery charge: Rs {delivery_charge}\n")
			total = items_total + delivery_charge
		f.write(f"TOTAL PAYABLE: Rs {total}\n")
	print(f"Receipt saved to {fname}")


def main():
	cart = {}
	print("Welcome to the Console Shopping Cart")
	add_to_cart(cart)
	if not cart:
		print("No items added. Exiting.")
		return
	show_cart(cart)
	# get customer details
	customer = get_customer_details()
	charge, avail = calc_delivery_charge(customer['distance'])
	if not avail:
		print("\nNote: No delivery available for the provided distance.")
		ans = input("Do you want to proceed with pickup instead? (y/n): ").strip().lower()
		if ans != 'y':
			print("Order cancelled.")
			return
		else:
			charge = 0
			avail = True

	print_bill(cart, customer, charge, avail)
	save = input("Save receipt to file? (y/n): ").strip().lower()
	if save == 'y':
		save_receipt(cart, customer, charge, avail)
	print("Thank you for shopping!")


if __name__ == '__main__':
	try:
		main()
	except KeyboardInterrupt:
		print("\nInterrupted. Exiting.")
		sys.exit(1)

